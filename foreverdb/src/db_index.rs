use super::*;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, Copy, PartialEq)]
pub struct IndexEntry {
    pub data_offset: u64,
    pub data_len: u32,
}

pub struct DBIndex {
    db: gdbm::Gdbm,
}

impl DBIndex {
    pub fn open(path: &Path) -> Result<Self> {
        let db =
            gdbm::Gdbm::new(path, 0, gdbm::Open::WRCREAT, libc::S_IRUSR | libc::S_IWUSR).unwrap();

        Ok(Self { db })
    }

    pub (super) fn insert(&mut self, k: &[u8], e: IndexEntry) -> Result<()> {
        let v = rkyv::to_bytes::<rkyv::rancor::Error>(&e).unwrap();
        self.db.store(k, &v, false).unwrap();
        Ok(())
    }

    pub (super) fn get(&self, k: &[u8]) -> Result<Option<IndexEntry>> {
        let data = self.db.fetch_data(k).unwrap();
        let v = rkyv::from_bytes::<IndexEntry, rkyv::rancor::Error>(&data).unwrap();
        Ok(Some(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let mut db = DBIndex::open(f.path()).unwrap();

        let key = vec![1;32];
        let val = IndexEntry {
            data_offset: 42,
            data_len: 100,
        };
        db.insert(&key, val).unwrap();

        let e = db.get(&key).unwrap();
        assert_eq!(e, Some(val));
    }
}
