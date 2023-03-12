/// Storage Engine Trait Definition
pub mod engine_interface;

/// Engine Error Definition
pub mod error;

/// Memory Engine Definition
pub mod memory_engine;

pub use self::engine_interface::{Delete, DeleteRange, Put, StorageEngine, WriteOperation};
