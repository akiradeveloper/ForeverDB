#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to open log")]
    LogOpenFailed,
    #[error("Failed to append to log")]
    LogAppendFailed,
    #[error("Failed to read from log")]
    LogReadFailed,
    #[error("Failed to open index")]
    IndexOpenFailed,
    #[error("Failed to insert into index")]
    IndexInsertFailed,
    #[error("Failed to get from index")]
    IndexGetFailed,
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Gdbm(#[from] gdbm::GdbmError),
}

pub type Result<T> = std::result::Result<T, Error>;
