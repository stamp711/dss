use std::thread::spawn;

use crossbeam_channel::Sender;
use futures::sync::mpsc::UnboundedSender;
use futures::sync::oneshot;
use futures::Future;

use labrpc::RpcFuture;

use crate::proto::raftpb::*;

use self::errors::*;
use self::event::Event;
use self::log::Log;
use self::persister::*;

#[cfg(test)]
pub mod config;
pub mod errors;
mod event;
mod log;
pub mod persister;
#[cfg(test)]
mod tests;

#[derive(Debug, Default)]
pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
    pub command_term: u64,
    pub ext: Option<ApplyMsgExt>,
    pub has_more_to_apply: bool,
}

#[derive(Debug)]
pub enum ApplyMsgExt {
    Stopped,
    ObtainLeadership,
    LostLeadership,
    RaftStateSize(usize),
    InstallSnapshot(Vec<u8>),
}

/// Describe the role of a raft peer
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
    Stopped,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(Message)]
struct PersistState {
    #[prost(uint64, required, tag = "1")]
    pub term: u64,
    #[prost(uint64, optional, tag = "2")]
    pub voted_for: Option<u64>,
    #[prost(message, required, tag = "3")]
    pub log: Log,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: u64,

    state: RaftState,
    term: u64,

    log: Log,
    commit_index: u64,
    apply_index: u64,

    apply_ch: UnboundedSender<ApplyMsg>,

    /// Indicates a pending persist operation.
    /// The pending persist must be performed before we communicate with the outside world.
    need_persist: bool,
    /// Indicates that we just saved raft state and have new persist size.
    have_new_persist_state_size: bool,
    /// The candidate that we voted for in the current term
    voted_for: Option<u64>,

    next_index: Vec<u64>,
    match_index: Vec<u64>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers\[me\]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me: me as u64,
            state: RaftState::Follower,
            term: 0,
            log: Log::new(),
            commit_index: 0,
            apply_index: 0,
            apply_ch,
            need_persist: false,
            have_new_persist_state_size: false,
            voted_for: None,
            next_index: Default::default(),
            match_index: Default::default(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        let state = self.get_persist_state();
        let mut data = vec![];
        labcodec::encode(&state, &mut data).unwrap();
        self.persister.save_raft_state(data);

        self.have_new_persist_state_size = true;
        self.need_persist = false;
    }

    fn get_persist_state(&self) -> PersistState {
        PersistState {
            term: self.term,
            voted_for: self.voted_for,
            log: self.log.clone(),
        }
    }

    fn load_persist_state(&mut self, state: PersistState) {
        self.term = state.term;
        self.voted_for = state.voted_for;
        self.log = state.log;

        // State machine should have already loaded the snapshot
        self.apply_index = self.log.start_index();
        self.commit_index = self.log.start_index();

        // ..and we should give initial raft state size to RSM
        self.have_new_persist_state_size = true;
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        match labcodec::decode(data) {
            Ok(state) => self.load_persist_state(state),
            Err(e) => panic!("{}", e),
        }
    }

    fn send_request_vote(
        &self,
        server: usize,
        args: &RequestVoteArgs,
        tx: Sender<RequestVoteReply>,
    ) {
        let peer = &self.peers[server];
        peer.spawn(
            peer.request_vote(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    if let Ok(res) = res {
                        let _ = tx.send(res);
                    };
                    Ok(())
                }),
        );
    }

    fn send_append_entries_and_map_reply<F, M>(
        &self,
        server: usize,
        args: AppendEntriesArgs,
        f: F,
        tx: Sender<M>,
    ) where
        F: FnOnce(AppendEntriesArgs, AppendEntriesReply) -> M + Send + 'static,
        M: Send + 'static,
    {
        let peer = &self.peers[server];
        peer.spawn(
            peer.append_entries(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    if let Ok(res) = res {
                        let _ = tx.send(f(args, res));
                    }
                    Ok(())
                }),
        );
    }

    fn send_install_snapshot_and_map_reply<F, M>(
        &self,
        server: usize,
        args: InstallSnapshotArgs,
        f: F,
        tx: Sender<M>,
    ) where
        F: FnOnce(InstallSnapshotArgs, InstallSnapshotReply) -> M + Send + 'static,
        M: Send + 'static,
    {
        let peer = &self.peers[server];
        peer.spawn(
            peer.install_snapshot(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    if let Ok(res) = res {
                        let _ = tx.send(f(args, res));
                    }
                    Ok(())
                }),
        );
    }
}

#[derive(Clone)]
pub struct Node {
    tx: Sender<Event>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let id = raft.me;
        let (tx, rx) = crossbeam_channel::unbounded();

        let event_tx = tx.clone();
        let _ = spawn(move || raft.event_loop(rx, event_tx));

        tx.send(Event::Hello {
            msg: format!("Raft {} is online!", id),
        })
        .unwrap();

        Node { tx }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let mut command_buf = vec![];
        labcodec::encode(command, &mut command_buf).map_err(Error::Encode)?;

        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Event::StartCommand {
                command: command_buf,
                tx,
            })
            .unwrap_or_default();
        rx.wait().unwrap_or(Err(Error::NotLeader))
    }

    pub fn save_snapshot(&self, starting_index: u64, data: Vec<u8>) {
        let _ = self.tx.send(Event::SaveSnapshot {
            starting_index,
            data,
        });
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Event::GetState { tx }).unwrap_or_default();
        rx.wait().unwrap_or_default()
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        self.tx.send(Event::Stop).unwrap();
    }
}

// CAVEATS: Please avoid locking or sleeping here, it may jam the network.
impl RaftService for Node {
    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Event::AppendEntries { args, tx })
            .unwrap_or_default();
        Box::new(rx.map_err(labrpc::Error::Recv))
    }
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Event::RequestVote { args, tx })
            .unwrap_or_default();
        Box::new(rx.map_err(labrpc::Error::Recv))
    }
    fn install_snapshot(&self, args: InstallSnapshotArgs) -> RpcFuture<InstallSnapshotReply> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Event::InstallSnapshot { args, tx })
            .unwrap_or_default();
        Box::new(rx.map_err(labrpc::Error::Recv))
    }
}
