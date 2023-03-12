use std::io;

use thiserror::Error;

/// The `EngineError` type represents all possible errors that can occur
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum EngineError {
    /// Met I/O Error during persisting data
    #[error("I/O Error")]
    IoError(#[from] io::Error),
    /// Table Not Found
    #[error("Table {0} Not Found")]
    TableNotFound(String),
    /// DB File Corrupted
    #[error("DB File {0} Corrupted")]
    Corruption(String),
    /// Invalid Argument Error
    #[error("Invalid Argument: {0}")]
    InvalidArgument(String),
    /// The Underlying Database Error
    #[error("The Undering Database Error: {0}")]
    UnderlyingDatabaseError(String),
    /// Key Not Found
    #[error("Key {0} Not Found")]
    KeyNotFound(String),
}
