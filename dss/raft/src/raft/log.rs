use crate::proto::raftpb::{LogEntry, LogInfo};

impl LogInfo {
    pub fn is_more_update_than(&self, other: &Self) -> bool {
        self.term > other.term || (self.term == other.term && self.index > other.index)
    }
}

#[derive(Clone, Message)]
pub struct Log {
    #[prost(uint64, tag = "1")]
    pub start_index: u64,
    #[prost(message, repeated, tag = "2")]
    pub entries: Vec<LogEntry>,
}

impl Log {
    pub fn new() -> Self {
        Self {
            start_index: 0,
            entries: vec![LogEntry::default()],
        }
    }

    pub fn _new_with_prev_info(info: LogInfo) -> Self {
        Self {
            start_index: info.index,
            entries: vec![LogEntry {
                info,
                command: Default::default(),
            }],
        }
    }

    pub fn start_index(&self) -> u64 {
        self.start_index
    }

    pub fn discard_logs_before(&mut self, index: u64) {
        let end = (index - self.start_index) as usize;
        self.entries.drain(0..end);
        self.start_index = index
    }

    pub fn get_log_info(&self, index: u64) -> LogInfo {
        if index < self.start_index {
            // Trying to get a info that's already been discarded
            unimplemented!("return Err(e)")
        } else {
            self.entries[(index - self.start_index) as usize]
                .info
                .clone()
        }
    }

    pub fn get_last_log_info(&self) -> LogInfo {
        self.entries
            .last()
            .unwrap() // self.entries must not be empty at all times
            .info
            .clone()
    }

    pub fn check_existence(&self, info: &LogInfo) -> bool {
        let index = info.index;
        let idx = (index - self.start_index) as usize;
        // Check boundary
        if index < self.start_index || idx >= self.entries.len() {
            false
        } else {
            self.entries[idx].info == *info
        }
    }

    pub fn get_next_index(&self) -> u64 {
        self.start_index + self.entries.len() as u64
    }

    pub fn update_logs_after(&mut self, index: u64, entries: &[LogEntry]) -> bool {
        let begin_index = index + 1;
        let end_index = begin_index + entries.len() as u64;

        if begin_index == end_index {
            return false;
        }

        if begin_index <= self.start_index {
            unimplemented!("Should return some kind of error or panic (out of bounds)")
        }

        let begin_idx = (begin_index - self.start_index) as usize;
        let end_idx = (end_index - self.start_index) as usize;

        if end_idx > self.entries.len() {
            self.entries.splice(begin_idx.., entries.iter().cloned());
        } else {
            self.entries
                .splice(begin_idx..end_idx, entries.iter().cloned());
        };

        if end_idx < self.entries.len()
            && self.entries[end_idx].info.term < self.entries[end_idx - 1].info.term
        {
            self.entries.truncate(end_idx);
        }

        true
    }

    pub fn get_entries(&self, begin_index: u64, end_index: u64) -> Vec<LogEntry> {
        let begin_idx = (begin_index - self.start_index) as usize;
        let end_idx = (end_index - self.start_index) as usize;

        if begin_index <= self.start_index || end_idx > self.entries.len() {
            panic!(
                "out of bounds. Entries idx is 0:{}, args {}:{}",
                self.entries.len(),
                begin_idx,
                end_idx
            );
        }

        self.entries[begin_idx..end_idx].to_vec()
    }

    pub fn get_entries_starting_at(&self, index: u64) -> Vec<LogEntry> {
        let idx = (index - self.start_index) as usize;
        if index <= self.start_index || idx > self.entries.len() {
            panic!("Out of bounds")
        }
        if idx < self.entries.len() {
            self.entries[idx..].to_vec()
        } else {
            vec![]
        }
    }

    pub fn append_command(&mut self, term: u64, command: Vec<u8>) -> LogInfo {
        self.entries.push(LogEntry {
            command,
            info: LogInfo {
                term,
                index: self.get_next_index(),
            },
        });
        self.get_last_log_info()
    }

    pub fn get_conflicting_index(&self, index: u64) -> u64 {
        let idx = (index - self.start_index) as usize;

        if index < self.start_index {
            // Log with that index is snapshotted, indicate the leader to send logs after the snapshot
            self.start_index + 1
        } else if idx < self.entries.len() {
            // If we have entry with that index, find minimum index of same term, but with index > log.SIndex
            let mut info = self.get_log_info(index);
            while info.index - 1 > self.start_index
                && self.get_log_info(info.index - 1).term == info.term
            {
                info.index -= 1;
            }
            info.index
        } else {
            // If we don't have entry with that index, indicate leader that nextIndex should be our last log's index +1
            self.get_last_log_info().index + 1
        }
    }
}
