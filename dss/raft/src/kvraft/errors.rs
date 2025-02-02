use std::{error, fmt, result};

use futures::Future;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    NoLeader,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Error::NoLeader => None,
        }
    }
}

pub type Result<T> = result::Result<T, Error>;
pub type KvRaftFuture<T> = Box<dyn Future<Item = T, Error = Error>>;
