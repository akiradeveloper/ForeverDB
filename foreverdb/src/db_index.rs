use super::*;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, Copy, PartialEq)]
pub struct IndexEntry {
    pub data_offset: u64,
    pub data_len: u32,
}

pub struct DBIndex {
    db: linhash::LinHash,
}

impl DBIndex {
    pub fn open(main: &Path, overflow: &Path) -> Result<Self> {
        let db = linhash::LinHash::new(main, overflow)?;

        Ok(Self { db })
    }

    pub(super) fn insert(&mut self, k: Vec<u8>, e: IndexEntry) -> Result<()> {
        let v = rkyv::to_bytes::<rkyv::rancor::Error>(&e).unwrap();
        self.db.insert(k, v.into_vec())?;
        Ok(())
    }

    pub(super) fn get(&self, k: &[u8]) -> Result<Option<IndexEntry>> {
        let Some(data) = self.db.get(k)? else {
            return Ok(None);
        };
        let v = rkyv::from_bytes::<IndexEntry, rkyv::rancor::Error>(&data).unwrap();
        Ok(Some(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let f1 = tempfile::NamedTempFile::new().unwrap();
        let f2 = tempfile::NamedTempFile::new().unwrap();
        let mut db = DBIndex::open(f1.path(), f2.path()).unwrap();

        let key = vec![1; 32];
        let val = IndexEntry {
            data_offset: 42,
            data_len: 100,
        };
        db.insert(key.clone(), val).unwrap();

        let e = db.get(&key).unwrap();
        assert_eq!(e, Some(val));
    }
}
