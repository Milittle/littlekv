use crate::error::EngineError;

/// Write operation
#[non_exhaustive]
#[derive(Debug)]
pub enum WriteOperation<'a> {
    /// `Put` operation
    Put(Put<'a>),
    /// `Delete` operation
    Delete(Delete<'a>),
    /// `DeleteRange` operation
    DeleteRange(DeleteRange<'a>),
}

/// `Put` operation
#[allow(dead_code)]
#[derive(Debug)]
pub struct Put<'a> {
    /// The table name
    pub table: &'a str,
    /// The key
    pub key: Vec<u8>,
    /// The value
    pub value: Vec<u8>,
    /// If true, the write will be flushed from the operating system buffer cache
    /// before the write is considered complete. If this flag is true, writes will
    /// be slower.
    pub sync: bool,
}

impl<'a> Put<'a> {
    /// Create a new `Put` operation
    #[inline]
    #[must_use]
    pub fn new(table: &'a str, key: Vec<u8>, value: Vec<u8>, sync: bool) -> Self {
        Self {
            table,
            key,
            value,
            sync,
        }
    }
}

/// Delete operation
#[allow(dead_code)]
#[derive(Debug)]
pub struct Delete<'a> {
    /// The table name
    pub(crate) table: &'a str,
    /// The key
    pub(crate) key: &'a [u8],
    /// If true, the write will be flushed from the operating system buffer cache
    /// before the write is considered complete. If this flag is true, writes will
    /// be slower.
    pub(crate) sync: bool,
}

impl<'a> Delete<'a> {
    /// Create a new `Delete` operation
    #[inline]
    #[must_use]
    pub fn new(table: &'a str, key: &'a [u8], sync: bool) -> Self {
        Self { table, key, sync }
    }
}

/// DeleteRange operation, it will remove the database
/// entries in the range [start_key, end_key).
#[allow(dead_code)]
#[derive(Debug)]
pub struct DeleteRange<'a> {
    /// The table name
    pub(crate) table: &'a str,
    /// The start key
    pub(crate) start_key: &'a [u8],
    /// The end key
    pub(crate) end_key: &'a [u8],
    /// If true, the write will be flushed from the operating system buffer cache
    /// before the write is considered complete. If this flag is true, writes will
    /// be slower.
    pub(crate) sync: bool,
}

impl<'a> DeleteRange<'a> {
    /// Create a new `DeleteRange` operation
    #[inline]
    #[must_use]
    pub fn new(table: &'a str, start_key: &'a [u8], end_key: &'a [u8], sync: bool) -> Self {
        Self {
            table,
            start_key,
            end_key,
            sync,
        }
    }
}

/// The `Storage` trait defines the interface for a storage engine.
pub trait StorageEngine: Send + Sync + 'static + std::fmt::Debug {
    /// Create a logical table with given name.
    ///
    /// # Errors
    /// Return `IoError` if met some io error.
    fn create_table(&self, table: &str) -> Result<(), EngineError>;

    /// Get the value associated with the given key and table.
    ///
    /// # Errors
    /// Return `TableNotFound` if the table is not found.
    /// Return `KeyNotFound` if the key is not found.
    /// Return `IoError` if met some io error.
    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError>;

    /// Get the values associated with the given keys and table.
    ///
    /// # Errors
    /// Return `TableNotFound` if the table is not found.
    /// Return `KeyNotFound` if the key is not found.
    /// Return `IoError` if met some io error.
    fn get_multi(
        &self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError>;

    /// Commit a batch of write operations.
    ///
    /// # Errors
    /// Return `TableNotFound` if the table is not found.
    /// Return `IoError` if met some io error.
    fn write_batch(&self, operations: Vec<WriteOperation<'_>>) -> Result<(), EngineError>;
}
