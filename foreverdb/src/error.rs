#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Log magic mismatch")]
    LogMagicMismatch,
    #[error("Log CRC mismatch")]
    LogCrcMismatch,
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Gdbm(#[from] gdbm::GdbmError),
}

pub type Result<T> = std::result::Result<T, Error>;
