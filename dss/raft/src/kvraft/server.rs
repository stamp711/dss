use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::spawn;

use futures::future::ok;
use futures::sync::mpsc::{unbounded, UnboundedReceiver};
use futures::sync::oneshot;
use futures::{Future, Stream};

use labrpc::RpcFuture;

use crate::proto::kvraftpb::*;
use crate::raft;

/// States that are shared between apply_handler and RPC threads
#[derive(Debug, Default)]
struct SharedServerState {
    /// Storage for KV pairs
    pub kvs: Arc<RwLock<HashMap<String, String>>>,
    /// Outstanding operations started in raft (index -> Rx<term>)
    pub req: Mutex<HashMap<u64, oneshot::Sender<u64>>>,
    /// Last seen seqID of each client
    pub latest: RwLock<HashMap<String, u64>>,
}

impl SharedServerState {
    pub fn rpc_seen(&self, info: &RpcId) -> bool {
        if let Some(seq_id) = self.latest.read().unwrap().get(&info.client_name) {
            *seq_id == info.seq_id
        } else {
            false
        }
    }

    pub fn rpc_needs_apply(&self, info: &RpcId) -> bool {
        match self.latest.read().unwrap().get(&info.client_name) {
            Some(seq_id) => info.seq_id > *seq_id,
            None => true,
        }
    }

    pub fn apply_command(&self, cmd: KvCommand) {
        if self.rpc_needs_apply(&cmd.info) {
            match cmd.command {
                c if c == CommandType::Get as i32 => {}
                c if c == CommandType::Put as i32 => {
                    self.kvs.write().unwrap().insert(cmd.key, cmd.value);
                }
                c if c == CommandType::Append as i32 => {
                    let mut guard = self.kvs.write().unwrap();
                    let value = guard.get_mut(&cmd.key);
                    match value {
                        Some(val_ref) => {
                            *val_ref += &cmd.value;
                        }
                        None => {
                            guard.insert(cmd.key, cmd.value);
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
        // Update latest seq_id
        self.latest
            .write()
            .unwrap()
            .insert(cmd.info.client_name, cmd.info.seq_id);
    }

    pub fn notify(&self, index: u64, term: u64) {
        if let Some(tx) = self.req.lock().unwrap().remove(&index) {
            let _ = tx.send(term);
        }
    }
}

#[allow(dead_code)] // TODO: remove
pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    apply_ch: Option<UnboundedReceiver<raft::ApplyMsg>>,
    apply_index: u64,
    last_do_snapshot_index: u64,
    state: Arc<SharedServerState>,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);
        let raft_node = raft::Node::new(rf);

        KvServer {
            rf: raft_node,
            me,
            maxraftstate,
            apply_ch: Some(apply_ch),
            apply_index: 0,
            last_do_snapshot_index: 0,
            state: Default::default(),
        }
    }

    pub fn apply_handler(mut self) {
        for msg in self.apply_ch.take().unwrap().wait() {
            let msg = msg.unwrap();
            if msg.command_valid {
                let kv_cmd = labcodec::decode(&msg.command).unwrap();
                self.state.apply_command(kv_cmd);
                self.state.notify(msg.command_index, msg.command_term);
                self.apply_index = msg.command_index;
            }
        }
    }
}

#[derive(Clone)]
pub struct Node {
    rf: raft::Node,
    state: Arc<SharedServerState>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        let node = Node {
            rf: kv.rf.clone(),
            state: kv.state.clone(),
        };
        let _ = spawn(move || kv.apply_handler());
        node
    }

    /// the tester calls Kill() when a KVServer instance won't be needed again.
    pub fn kill(&self) {
        // Your code here, if desired.
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        raft::State {
            ..Default::default()
        }
    }

    fn submit_to_raft(&self, command: &KvCommand) -> RpcFuture<(bool, Option<String>)> {
        let mut req_guard = self.state.req.lock().unwrap();

        // Call start on raft node
        if let Ok((index, submit_term)) = self.rf.start(command) {
            // If started, register to outstanding requests
            let (req_tx, req_rx) = oneshot::channel();
            req_guard.insert(index, req_tx);
            drop(req_guard);
            // Process future res
            let res = req_rx.map(move |term| {
                if term == submit_term {
                    // Successfully applied our command
                    (false, None)
                } else {
                    // Applied command from other term, or recv term 0
                    (false, Some("LostLeadership".to_string()))
                }
            });
            Box::new(res.map_err(labrpc::Error::Recv))
        } else {
            // Failed to start command
            Box::new(ok((true, Some("WrongLeader".to_string()))))
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
enum CommandType {
    Get = 0,
    Put = 1,
    Append = 2,
}

#[derive(Message)]
struct KvCommand {
    #[prost(message, required, tag = "1")]
    info: RpcId,
    #[prost(enumeration = "CommandType", required, tag = "2")]
    command: i32,
    #[prost(string, required, tag = "3")]
    key: String,
    #[prost(string, required, tag = "4")]
    value: String,
}

impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn get(&self, args: GetRequest) -> RpcFuture<GetReply> {
        // Check for duplicate
        if self.state.rpc_seen(&args.info) {
            // If seen, directly read and return
            let value = self.state.kvs.read().unwrap().get(&args.key).cloned();
            return Box::new(ok(GetReply {
                wrong_leader: false,
                err: None,
                value: value.unwrap_or_default(),
            }));
        }

        // Submit to Raft
        let kvs = self.state.kvs.clone();
        let key = args.key.clone();
        Box::new(
            self.submit_to_raft(&KvCommand {
                info: args.info,
                command: CommandType::Get as i32,
                ..Default::default()
            })
            .map(move |(wrong_leader, err)| {
                let value = if err.is_none() {
                    kvs.read().unwrap().get(&key).cloned().unwrap_or_default()
                } else {
                    "".to_string()
                };
                GetReply {
                    wrong_leader,
                    err,
                    value,
                }
            }),
        )
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn put_append(&self, args: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        // Check for duplicate
        if self.state.rpc_seen(&args.info) {
            return Box::new(ok(PutAppendReply {
                wrong_leader: false,
                err: None,
            }));
        }

        // Submit to Raft
        let raft_cmd = KvCommand {
            info: args.info,
            command: match args.op {
                i if i == Op::Put as i32 => CommandType::Put as i32,
                i if i == Op::Append as i32 => CommandType::Append as i32,
                _ => unreachable!(),
            },
            key: args.key,
            value: args.value,
        };
        Box::new(
            self.submit_to_raft(&raft_cmd)
                .map(|(wrong_leader, err)| PutAppendReply { wrong_leader, err }),
        )
    }
}
