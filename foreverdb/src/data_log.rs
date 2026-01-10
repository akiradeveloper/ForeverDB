use super::*;

pub struct DataLog {}

impl DataLog {
    pub fn open(path: &Path) -> Self {
        todo!()
    }

    // Appends data to the log and returns the offset where the data was written.
    pub (super) fn append(&mut self, data: &[u8]) -> Result<u64> {
        todo!()
    }

    pub (super) fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_and_read() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let mut log = DataLog::open(f.path());

        let data1 = vec![1;10];
        log.append(&data1).unwrap();
        let data2 = vec![2;20];
        let offset2 = log.append(&data2).unwrap();

        let read_data = log.read(offset2, data2.len() as u32).unwrap();
        assert_eq!(read_data, data2);
    }
}