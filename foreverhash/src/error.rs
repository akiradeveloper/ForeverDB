#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Key already exists")]
    KeyAlreadyExists,
    #[error(transparent)]
    Rkyv(#[from] rkyv::rancor::Error),
    #[error(transparent)]
    IO(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
