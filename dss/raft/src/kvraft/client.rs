use std::cell::Cell;
use std::fmt;
use std::thread::sleep;
use std::time::Duration;

use futures::Future;
use rand::Rng;

use crate::proto::kvraftpb::Op as RpcOp;
use crate::proto::kvraftpb::*;

#[derive(Debug, Clone)]
enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    leader: Cell<u64>,
    seq_id: Cell<u64>,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

const RETRY_INTERVAL: Duration = Duration::from_millis(20);

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        Clerk {
            name,
            servers,
            leader: Cell::new(0),
            seq_id: Cell::new(0),
        }
    }

    fn gen_rpc_info(&self) -> RpcId {
        let info = RpcId {
            client_name: self.name.clone(),
            seq_id: self.seq_id.get(),
        };
        self.seq_id.set(self.seq_id.get() + 1);
        info
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].get(args).unwrap();
    pub fn get(&self, key: String) -> String {
        let args = GetRequest {
            info: self.gen_rpc_info(),
            key,
        };
        debug!("Clerk {} Get {} start", self.name, args.key);
        loop {
            let reply = self.servers[self.leader.get() as usize].get(&args).wait();
            if let Ok(reply) = reply {
                if reply.err.is_none() {
                    debug!("Clerk {} Get {} ok", self.name, args.key);
                    return reply.value;
                }
            }
            self.leader
                .set(rand::thread_rng().gen_range(0, self.servers.len() as u64));
            sleep(RETRY_INTERVAL);
        }
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        let args = match op {
            Op::Put(key, value) => PutAppendRequest {
                info: self.gen_rpc_info(),
                op: RpcOp::Put as i32,
                key,
                value,
            },
            Op::Append(key, value) => PutAppendRequest {
                info: self.gen_rpc_info(),
                op: RpcOp::Append as i32,
                key,
                value,
            },
        };
        debug!("Clerk {} PutAppend {} start", self.name, args.key);
        loop {
            let reply = self.servers[self.leader.get() as usize]
                .put_append(&args)
                .wait();
            if let Ok(reply) = reply {
                if reply.err.is_none() {
                    debug!("Clerk {} PutAppend {} ok", self.name, args.key);
                    return;
                }
            }
            self.leader
                .set(rand::thread_rng().gen_range(0, self.servers.len() as u64));
            sleep(RETRY_INTERVAL);
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
