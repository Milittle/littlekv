use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use crate::engine_interface::{Delete, DeleteRange, Put, StorageEngine, WriteOperation};
use crate::error::EngineError;

type MemoryTable = HashMap<Vec<u8>, Vec<u8>>;

#[derive(Clone, Default, Debug)]
pub struct MemoryEngine {
    inner: Arc<RwLock<HashMap<String, MemoryTable>>>,
}

impl MemoryEngine {
    /// New ``MemoryEngine`` instance
    ///
    /// # Example
    ///
    /// Returns `EngineError` when DB create tables failed or open failed.
    #[inline]
    pub fn new(tables: &[&'static str]) -> Result<Self, EngineError> {
        let mut inner: HashMap<String, HashMap<Vec<u8>, Vec<u8>>> = HashMap::new();
        for table in tables {
            inner.entry((*table).to_owned()).or_insert(HashMap::new());
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }
}

/// StorageEngine trait implementation for MemoryEngine
impl StorageEngine for MemoryEngine {
    /// Create table
    #[inline]
    fn create_table(&self, table: &str) -> Result<(), EngineError> {
        let mut inner = self.inner.write();
        inner.entry(table.to_owned()).or_insert(HashMap::new());
        Ok(())
    }

    /// Get value by key
    #[inline]
    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError> {
        let inner = self.inner.read();
        let table = inner
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
        let value = table.get(&key.as_ref().to_vec()).ok_or_else(|| {
            EngineError::KeyNotFound(String::from_utf8(key.as_ref().to_vec()).unwrap())
        })?;
        Ok(Some(value.clone()))
    }

    /// Get values by keys
    #[inline]
    fn get_multi(
        &self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        let inner = self.inner.read();
        let table = inner
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(table.get(&key.as_ref().to_vec()).cloned());
        }
        Ok(values)
    }

    /// Write operation
    #[inline]
    fn write_batch(&self, wr_ops: Vec<WriteOperation<'_>>) -> Result<(), EngineError> {
        let mut inner = self.inner.write();
        for op in wr_ops {
            match op {
                WriteOperation::Put(Put {
                    table, key, value, ..
                }) => {
                    let table = inner
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                    table.insert(key, value);
                }
                WriteOperation::Delete(Delete { table, key, .. }) => {
                    let table = inner
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                    table.remove(key);
                }
                WriteOperation::DeleteRange(DeleteRange {
                    table,
                    start_key,
                    end_key,
                    ..
                }) => {
                    let table = inner
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                    table.retain(|k, _| {
                        let key_slice = k.as_slice();
                        match (key_slice.cmp(start_key), key_slice.cmp(end_key)) {
                            (Ordering::Greater, Ordering::Less) => false,
                            _ => true,
                        }
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::iter::{repeat, zip};

    use super::*;
    use crate::engine_interface::Put;

    const TESTTABLES: [&'static str; 3] = ["testtable1", "testtable2", "testtable3"];

    #[test]
    fn write_batch_into_a_non_existing_table_should_fail() {
        let engine = MemoryEngine::new(&TESTTABLES).unwrap();
        let wr_ops = vec![WriteOperation::Put(Put {
            table: "testtable4",
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            sync: false,
        })];
        assert!(engine.write_batch(wr_ops).is_err());

        // delete operation
        let wr_ops = vec![WriteOperation::Delete(Delete {
            table: "testtable4",
            key: b"key",
            sync: false,
        })];
        let res = engine.write_batch(wr_ops);
        assert!(res.is_err());
        println!("{:?}", res);

        // delete range operation
        let wr_ops = vec![WriteOperation::DeleteRange(DeleteRange {
            table: "testtable4",
            start_key: b"start_key",
            end_key: b"end_key",
            sync: false,
        })];
        assert!(engine.write_batch(wr_ops).is_err());
    }

    #[test]
    fn write_batch_should_success() {
        let engine = MemoryEngine::new(&TESTTABLES).unwrap();
        engine.create_table("table").unwrap();

        let origin_set: Vec<Vec<u8>> = (1u8..=10u8)
            .map(|val| repeat(val).take(4).collect())
            .collect();

        let keys = origin_set.clone();
        let values = origin_set.clone();
        let puts = zip(keys, values)
            .map(|(key, value)| {
                WriteOperation::Put(Put {
                    table: "table",
                    key,
                    value,
                    sync: false,
                })
            })
            .collect::<Vec<WriteOperation<'_>>>();

        assert!(engine.write_batch(puts).is_ok());

        let res_1 = engine.get_multi("table", &origin_set).unwrap();
        assert_eq!(res_1.iter().filter(|v| v.is_some()).count(), 10);

        let delete_keys = vec![1, 1, 1, 1];
        let deletes = WriteOperation::Delete(Delete::new("table", delete_keys.as_slice(), false));

        let res_2 = engine.write_batch(vec![deletes]);
        assert!(res_2.is_ok());

        let res_3 = engine.get("table", &delete_keys);
        assert!(res_3.is_err());

        let delete_start: Vec<u8> = vec![2, 2, 2, 2];
        let delete_end: Vec<u8> = vec![5, 5, 5, 5];
        let delete_range = WriteOperation::DeleteRange(DeleteRange::new(
            "table",
            delete_start.as_slice(),
            delete_end.as_slice(),
            false,
        ));
        let res_4 = engine.write_batch(vec![delete_range]);
        assert!(res_4.is_ok());

        let get_key_1 = vec![2, 2, 2, 2];
        let get_key_2 = vec![5, 5, 5, 5];
        let get_key_3 = vec![3, 3, 3, 3];
        assert!(engine.get("table", &get_key_1).unwrap().is_some());
        assert!(engine.get("table", &get_key_2).unwrap().is_some());
        let res_5 = engine.get("table", &get_key_3);
        println!("{:?}", res_5);
        assert!(res_5.is_err());
    }
}
