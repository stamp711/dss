use std::time::{Duration, Instant};

use crossbeam_channel::{after, Receiver, Select, Sender};
use futures::sync::oneshot;
use rand::{thread_rng, Rng};

use crate::proto::raftpb::*;
use crate::raft::errors::*;
use crate::raft::RaftState::*;
use crate::raft::{ApplyMsg, Raft, State};

const START_BATCH_MS: u64 = 2;
const COMMIT_BATCH_MS: u64 = 30;

#[derive(Debug)]
pub enum Event {
    Hello {
        msg: String,
    },
    Stop,
    GetState {
        tx: oneshot::Sender<State>,
    },
    StartCommand {
        command: Vec<u8>,
        tx: oneshot::Sender<Result<(u64, u64)>>,
    },
    AppendEntries {
        args: AppendEntriesArgs,
        tx: oneshot::Sender<AppendEntriesReply>,
    },
    RequestVote {
        args: RequestVoteArgs,
        tx: oneshot::Sender<RequestVoteReply>,
    },
    UpdateFollowerSummary(UpdateFollowerSummary),
}

pub enum Action {
    ShouldResetTimer,
    LeaderCommitUpdated,
    CommandStarted,
}

macro_rules! persist {
    ($raft:tt) => {
        if $raft.need_persist {
            $raft.persist();
        }
    };
}

macro_rules! persist_and_ack {
    ($raft:tt, $tx:tt, $reply:tt) => {
        persist!($raft);
        let _ = $tx.send($reply);
    };
}

impl Raft {
    pub fn event_loop(mut self, events: Receiver<Event>, tx: Sender<Event>) {
        while self.state != Stopped {
            match self.state {
                Leader => self.leader_loop(&events, &tx),
                Candidate => self.candidate_loop(&events),
                Follower => self.follower_loop(&events),
                Stopped => {}
            }
        }

        // After Raft state transfers to Stopped, though events rx will be dropped,
        // pending events in the channel buffer will not be cleaned.
        // When state transfers to Stopped, we need to ensure clean shutdown.

        // Drop events Sender
        drop(tx);
        // Cancel all remaining events while channel is still connected (have other Senders)
        events.iter().for_each(drop);
        debug!("Raft {} reached clean shutdown", self.me);
    }

    fn leader_loop(&mut self, events: &Receiver<Event>, tx: &Sender<Event>) {
        // Reinitialize after election
        self.next_index = vec![self.log.get_next_index(); self.peers.len()];
        self.match_index = vec![0u64; self.peers.len()];

        // Broadcast AppendEntries right after leadership establishment
        let mut followers_need_update = true;

        let mut last_broadcast = Instant::now();
        let mut timeout = gen_heartbeat_interval();

        while self.state == Leader {
            // 1. Update followers if needed
            if followers_need_update {
                debug!("{:?} since last broadcast", Instant::now() - last_broadcast);
                persist!(self);
                self.broadcast_append_entries(tx.clone());
                last_broadcast = Instant::now();
                followers_need_update = false;
                // Set new timeout
                timeout = gen_heartbeat_interval();
            }

            // 2. Apply if needed
            if self.need_apply {
                self.update_state_machine();
            }

            // 3. Wait for Event, UpdateSummary or timeout
            let deadline = after(timeout);
            let mut sel = Select::new();
            let op_timeout = sel.recv(&deadline);
            let op_event = sel.recv(events);
            let sel_start = Instant::now();

            let op = sel.select();
            match op.index() {
                // Heartbeat deadline reached
                i if i == op_timeout => {
                    let _ = op.recv(&deadline);
                    followers_need_update = true;
                }
                // Event
                i if i == op_event => match op.recv(events) {
                    Err(_) => unreachable!(),
                    Ok(event) => {
                        // Process event
                        let action = self.process_event(event);

                        // Timeout subtraction
                        timeout = timeout
                            .checked_sub(Instant::now().duration_since(sel_start))
                            .unwrap_or_default();

                        // If LeaderCommitUpdated or CommandStarted, make next timeout smaller
                        if let Some(action) = action {
                            match action {
                                Action::LeaderCommitUpdated => {
                                    if timeout > Duration::from_millis(COMMIT_BATCH_MS) {
                                        timeout = Duration::from_millis(COMMIT_BATCH_MS)
                                    }
                                }
                                Action::CommandStarted => {
                                    if timeout > Duration::from_millis(START_BATCH_MS) {
                                        timeout = Duration::from_millis(START_BATCH_MS)
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                },
                _ => unreachable!(),
            }
        }
    }

    fn candidate_loop(&mut self, events: &Receiver<Event>) {
        // Loop for each election round
        while self.state == Candidate {
            // 1. Start election for each term
            // 1.1 Advance term
            self.term += 1;
            self.voted_for = Some(self.me);
            self.need_persist = true;

            // 1.2 Start election
            let args = RequestVoteArgs {
                candidate_id: self.me,
                last_log_info: self.log.get_last_log_info(),
                term: self.term,
            };

            let mut votes = 1; // Received votes for this term. Because we voted for self, this starts at 1.

            // Make new vote reply channel for every term
            let (vote_tx, vote_rx) = crossbeam_channel::bounded(self.peers.len());

            // Send out the RequestVotes
            persist!(self); // Needs to perform pending persist before we communicate with the outside world...
            for (id, _) in self.peers.iter().enumerate() {
                if id as u64 != self.me {
                    self.send_request_vote(id, &args, vote_tx.clone());
                }
            }

            // After this deadline, the election times out
            let deadline = after(gen_random_timeout());

            // Loop for each event
            while self.state == Candidate {
                // 1. Apply if needed
                if self.need_apply {
                    self.update_state_machine();
                }

                // 2. Wait for an event, election reply, or election timeout
                let mut sel = Select::new();
                let op_vote = sel.recv(&vote_rx); // Vote reply
                let op_event = sel.recv(events); // Event
                let op_timeout = sel.recv(&deadline); // Election timeout

                let op = sel.select();
                match op.index() {
                    // Timeout
                    i if i == op_timeout => {
                        // If election timed out, start next round
                        let _ = op.recv(&deadline);
                        break; // To next election round
                    }
                    // Vote reply
                    i if i == op_vote => {
                        if let Ok(reply) = op.recv(&vote_rx) {
                            if reply.term > self.term {
                                // If found newer term, become Follower
                                self.update_term(reply.term);
                                break;
                            } else if reply.term == self.term && reply.vote_granted {
                                // If we get enough votes, become Leader
                                votes += 1;
                                if votes > self.peers.len() / 2 {
                                    debug!("{} becomes leader in term {}", self.me, self.term);
                                    self.state = Leader;
                                    break;
                                }
                            }
                        }
                    }
                    // Event
                    i if i == op_event => match op.recv(events) {
                        Err(_) => unreachable!(),
                        Ok(event) => {
                            let _ = self.process_event(event);
                        }
                    },
                    _ => unreachable!(),
                }
            }
        }
    }

    fn follower_loop(&mut self, events: &Receiver<Event>) {
        let mut timeout = gen_random_timeout();

        while self.state == Follower {
            // 1. Apply if needed
            if self.need_apply {
                self.update_state_machine();
            }

            // 2. Wait for an event or timeout
            let recv_start = Instant::now();
            let recv = events.recv_timeout(timeout);

            match recv {
                // Follower timeout, promote itself to Candidate
                Err(_) => {
                    // TODO: the recv could also fail because no Sender exists
                    // The follower times out, and promotes itself into Candidate
                    debug!("Follower {} timeout on term {}", self.me, self.term);
                    self.state = Candidate;
                    return;
                }

                // Recv event, process it and calculate remaining timeout
                Ok(event) => {
                    if let Some(Action::ShouldResetTimer) = self.process_event(event) {
                        timeout = gen_random_timeout();
                    } else {
                        timeout = timeout
                            .checked_sub(Instant::now().duration_since(recv_start))
                            .unwrap_or_default();
                    }
                }
            }
        }
    }

    fn process_event(&mut self, event: Event) -> Option<Action> {
        let mut action = None;

        match event {
            // Display a HELLO message
            Event::Hello { msg } => self.hello(&msg),

            // Stop the event loop
            Event::Stop => {
                self.state = Stopped;
            }

            Event::GetState { tx } => {
                let _ = tx.send(self.get_state());
            }

            Event::StartCommand { command, tx } => {
                let _ = tx.send(self.process_start_command(command));
                if self.state == Leader {
                    action = Some(Action::CommandStarted);
                }
            }

            Event::AppendEntries { args, tx } => {
                let reply = self.process_append_entries(&args);
                if self.state == Follower && args.term == self.term {
                    action = Some(Action::ShouldResetTimer);
                }
                persist_and_ack!(self, tx, reply);
            }

            Event::RequestVote { args, tx } => {
                let reply = self.process_request_vote(&args);
                if self.state == Follower && reply.vote_granted {
                    action = Some(Action::ShouldResetTimer);
                }
                persist_and_ack!(self, tx, reply);
            }

            Event::UpdateFollowerSummary(summary) => {
                if self.state == Leader {
                    let updated = self.process_update_follower_summary(&summary);
                    if updated && self.update_leader_commit() {
                        action = Some(Action::LeaderCommitUpdated);
                    }
                }
            }
        };

        action
    }

    fn process_start_command(&mut self, command: Vec<u8>) -> Result<(u64, u64)> {
        if self.state != Leader {
            Err(Error::NotLeader)
        } else {
            let info = self.log.append_command(self.term, command);
            self.need_persist = true;
            debug!("Start {:?}", info);
            Ok((info.index, info.term))
        }
    }

    /// Processes an AppendEntries event
    fn process_append_entries(&mut self, args: &AppendEntriesArgs) -> AppendEntriesReply {
        let mut reply = AppendEntriesReply::default();

        // If request's term is larger than mine, update my term
        if args.term > self.term {
            self.update_term(args.term);
        }

        // 1. Reply false if args.term < self.term
        if args.term < self.term {
            reply.term = self.term;
            reply.success = false;
            return reply;
        }

        // Now raft have same term as in args
        if self.state == Candidate {
            self.state = Follower;
        }

        // Reply false if the log entry has been truncated, telling the leader to send logs after snapshot
        if args.prev_log_info.index < self.log.start_index() {
            reply.term = self.term;
            reply.success = false;
            reply.conflicting_index = self.log.start_index();
            return reply;
        }

        // 2. Reply false if log don’t contain an entry at prevLogIndex whose term matches prevLogTerm
        if !self.log.check_existence(&args.prev_log_info) {
            reply.term = self.term;
            reply.success = false;
            reply.conflicting_index = self.log.get_conflicting_index(args.prev_log_info.index);
            return reply;
        }

        // Append entries to my logs
        let updated = self
            .log
            .update_logs_after(args.prev_log_info.index, &args.entries);
        self.need_persist = updated;

        // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if args.leader_commit > self.commit_index {
            self.commit_index = args.leader_commit;
            self.need_apply = true;
            debug!(
                "Follower {} commit_index advances to {}",
                self.me, self.commit_index
            );
        }

        // Reply success
        reply.term = self.term;
        reply.success = true;
        reply
    }

    /// Processes a RequestVote event
    fn process_request_vote(&mut self, args: &RequestVoteArgs) -> RequestVoteReply {
        let mut reply = RequestVoteReply::default();

        // If request's term is larger than mine, update my term
        if args.term > self.term {
            self.update_term(args.term);
        }

        // Reply false if args.term < self.term
        if args.term < self.term {
            debug!(
                "{} deny vote for {} (stale term)",
                self.me, args.candidate_id
            );
            reply.term = self.term;
            reply.vote_granted = false;
            return reply;
        }

        // Now raft have same term as in args

        // Deny if already voted for a different candidate
        if let Some(id) = self.voted_for {
            if id != args.candidate_id {
                debug!(
                    "{} deny vote for {} (already voted)",
                    self.me, args.candidate_id
                );
                reply.term = self.term;
                reply.vote_granted = false;
                return reply;
            }
        }

        // If candidate's log is not at least up-to-date as receiver's log, deny vote
        let my_last_log_info = self.log.get_last_log_info();
        if my_last_log_info.is_more_update_than(&args.last_log_info) {
            debug!(
                "[Vote] {} deny vote for {} (older log {:?} > {:?})",
                self.me, args.candidate_id, my_last_log_info, args.last_log_info
            );
            reply.term = self.term;
            reply.vote_granted = false;
            return reply;
        }

        // Grant vote
        self.need_persist = true;
        debug!(
            "{} grant vote for {} on term {}",
            self.me, args.candidate_id, self.term
        );
        self.voted_for = Some(args.candidate_id);
        reply.term = self.term;
        reply.vote_granted = true;
        reply
    }

    fn process_update_follower_summary(&mut self, summary: &UpdateFollowerSummary) -> bool {
        if self.state != Leader || summary.send_term != self.term {
            return false;
        }

        let peer = summary.peer as usize;
        let mut updated = false;

        if summary.peer_ok {
            updated = summary.expected_synced_log_index > self.match_index[peer];
            if updated {
                self.match_index[peer] = summary.expected_synced_log_index;
                self.next_index[peer] = summary.expected_synced_log_index + 1;
            }
        } else if summary.peer_conflicting_index > 0 {
            self.next_index[peer] = summary.peer_conflicting_index;
        } else {
            self.next_index[peer] -= 1;
            if self.next_index[peer] < 1 {
                self.next_index[peer] = 1;
            }
        }

        // Prevent nextIndex less than matchIndex+1 in last 2 cases
        // because we process UpdateFollowerSummary out of order so this could happen
        // e.g. a failed AppendEntriesReply processed after succeeded InstallSnapshotReply
        // actually peer's nextIndex will never decrease to less than rf.matchIndex[peer]+1,
        // or safety is violated
        if self.next_index[peer] < self.match_index[peer] + 1 {
            self.next_index[peer] = self.match_index[peer] + 1
        }

        updated
    }

    fn get_state(&self) -> State {
        State {
            term: self.term,
            is_leader: self.state == Leader,
        }
    }

    fn hello(&self, msg: &str) {
        debug!("HELLO: {}", msg);
    }

    // Helpers

    fn get_apply_messages(&mut self) -> Vec<ApplyMsg> {
        let mut msgs = vec![];

        if self.commit_index > self.apply_index {
            for log in self
                .log
                .get_entries(self.apply_index + 1, self.commit_index + 1)
            {
                msgs.push(ApplyMsg {
                    command_valid: true,
                    command: log.command,
                    command_index: log.info.index,
                });
            }
            self.apply_index = self.commit_index;
        }

        msgs
    }

    fn update_state_machine(&mut self) {
        let msgs = self.get_apply_messages();
        for msg in msgs {
            let _ = self.apply_ch.unbounded_send(msg);
        }
        debug!("{} updated RSM to index {}", self.me, self.apply_index);
        self.need_apply = false;
    }

    fn update_leader_commit(&mut self) -> bool {
        // First update match_index of leader from leader's log
        self.match_index[self.me as usize] = self.log.get_last_log_info().index;

        let mut indexes = self.match_index.clone();
        indexes.sort();

        let majority_index = indexes[(self.peers.len() - 1) / 2];
        if majority_index > self.commit_index
            && self.log.get_log_info(majority_index).term == self.term
        {
            self.commit_index = majority_index;
            self.need_apply = true;
            true
        } else {
            false
        }
    }

    /// Update to a newer term and become Follower
    fn update_term(&mut self, term: u64) {
        // Make sure term is larger than self.term
        if term <= self.term {
            return;
        }

        self.term = term;
        self.state = Follower;
        self.voted_for = None;
        self.need_persist = true;
    }

    /// Send AppendEntries to all other peers
    fn broadcast_append_entries(&self, tx: Sender<Event>) {
        for (id, _) in self.peers.iter().enumerate() {
            if id as u64 != self.me {
                let prev_log_index = self.next_index[id] - 1;
                if prev_log_index < self.log.start_index() {
                    // Send InstallSnapshot
                    let _args = InstallSnapshotArgs {
                        term: self.term,
                        leader_id: self.me,
                        last_included_info: self.log.get_log_info(self.log.start_index()),
                        data: self.persister.snapshot(),
                    };
                    unimplemented!()
                } else {
                    // Send AppendEntries
                    let args = AppendEntriesArgs {
                        term: self.term,
                        leader_id: self.me,
                        prev_log_info: self.log.get_log_info(prev_log_index),
                        entries: self.log.get_entries_starting_at(prev_log_index + 1),
                        leader_commit: self.commit_index,
                    };
                    self.update_follower(id as u64, args, tx.clone());
                }
            }
        }
    }

    fn update_follower(&self, peer: u64, args: AppendEntriesArgs, tx: Sender<Event>) {
        self.send_append_entries_and_map_reply(
            peer as usize,
            args,
            move |args, reply| {
                let mut summary = UpdateFollowerSummary {
                    send_term: args.term,
                    peer,
                    expected_synced_log_index: args.prev_log_info.index,
                    peer_ok: reply.success,
                    peer_term: reply.term,
                    peer_conflicting_index: reply.conflicting_index,
                };
                if let Some(log) = args.entries.last() {
                    summary.expected_synced_log_index = log.info.index;
                }
                Event::UpdateFollowerSummary(summary)
            },
            tx,
        );
    }
}

#[derive(Debug)]
pub struct UpdateFollowerSummary {
    send_term: u64,
    peer: u64,
    expected_synced_log_index: u64,
    peer_ok: bool,
    peer_term: u64,
    peer_conflicting_index: u64,
}

fn gen_heartbeat_interval() -> Duration {
    Duration::from_millis(120)
}

fn gen_random_timeout() -> Duration {
    Duration::from_millis(thread_rng().gen_range(300, 500))
}
