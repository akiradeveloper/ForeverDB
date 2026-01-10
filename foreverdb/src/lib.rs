mod error;
use error::{Error, Result};
use std::path::Path;

mod data_log;
use data_log::DataLog;
mod db_index;
use db_index::{DBIndex, IndexEntry};

pub struct ForeverDB {
    data_log: DataLog,
    db_index: DBIndex,
}

impl ForeverDB {
    pub fn new(data_log: DataLog, db_index: DBIndex) -> Self {
        Self { data_log, db_index }
    }

    pub fn insert(&mut self, key: &[u8], data: &[u8]) -> Result<()> {
        let data_offset = self.data_log.append(data)?;
        let data_len = data.len() as u32;

        self.db_index.insert(
            key,
            IndexEntry {
                data_offset,
                data_len,
            },
        )?;

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some(e) = self.db_index.get(key)? else {
            return Ok(None);
        };

        Ok(Some(self.data_log.read(e.data_offset, e.data_len)?))
    }

    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        Ok(self.db_index.get(key)?.is_some())
    }
}
