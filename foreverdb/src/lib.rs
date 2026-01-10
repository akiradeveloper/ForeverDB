mod error;
use error::{Error, Result};
use std::path::Path;

mod data_log;
pub use data_log::DataLog;
mod db_index;
pub use db_index::DBIndex;
use db_index::IndexEntry;

pub struct ForeverDB {
    data_log: DataLog,
    db_index: DBIndex,
}

impl ForeverDB {
    pub fn new(data_log: DataLog, db_index: DBIndex) -> Self {
        Self { data_log, db_index }
    }

    pub fn insert(&mut self, key: &[u8], data: &[u8]) -> Result<()> {
        let (data_offset, data_len) = self.data_log.append(data)?;

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

        Ok(Some(self.data_log.read((e.data_offset, e.data_len))?))
    }

    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        Ok(self.db_index.get(key)?.is_some())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.data_log.sync()?;
        self.db_index.sync();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let log_file = tempfile::NamedTempFile::new().unwrap();
        let index_file = tempfile::NamedTempFile::new().unwrap();

        let data_log = DataLog::open(log_file.path()).unwrap();
        let db_index = DBIndex::open(index_file.path()).unwrap();
        let mut db = ForeverDB::new(data_log, db_index);

        let k1 = vec![1; 32];
        let v1 = vec![42; 100];

        let k2 = vec![2; 32];
        let v2 = vec![43; 100];

        db.insert(&k1, &v1).unwrap();
        db.insert(&k2, &v2).unwrap();
        assert!(db.exists(&k1).unwrap());
        assert!(db.exists(&k2).unwrap());

        assert_eq!(db.get(&k1).unwrap().unwrap(), v1);
        assert_eq!(db.get(&k2).unwrap().unwrap(), v2);
    }
}
