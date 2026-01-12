use std::io::Write;
use std::os::unix::fs::FileExt;

use super::*;

const MAGIC: u32 = 0x34655652; // 4eVR
const HEADER_LEN: u32 = 4 + 4;

pub struct DataLog {
    f: std::fs::File,
    cursor: u64,
}

impl DataLog {
    pub fn open(path: &Path) -> Result<Self> {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path)?;

        // Get the current tail position.
        let meta = f.metadata()?;
        let cursor = meta.len();

        Ok(Self { f, cursor })
    }

    // Appends data to the log and returns the offset where the data was written.
    pub(super) fn append(&mut self, data: &[u8]) -> Result<(u64, u32)> {
        let data_len = data.len() as u32;

        let buf = {
            let crc = crc32fast::hash(data);

            let mut out = Vec::with_capacity(HEADER_LEN as usize + data_len as usize);
            out.extend_from_slice(&MAGIC.to_le_bytes());
            out.extend_from_slice(&crc.to_le_bytes());
            out.extend_from_slice(data);
            out
        };

        let offset = self.cursor;
        self.f.write_at(&buf, offset)?;
        self.cursor += HEADER_LEN as u64 + data_len as u64;

        Ok((offset, HEADER_LEN + data_len))
    }

    pub(super) fn read(&self, k: (u64, u32)) -> Result<Vec<u8>> {
        let (offset, len) = k;
        let mut buf = vec![0u8; len as usize];
        self.f.read_at(&mut buf, offset)?;

        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != MAGIC {
            return Err(Error::LogMagicMismatch);
        }

        let crc_stored = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let crc_calculated = crc32fast::hash(&buf[8..]);
        if crc_stored != crc_calculated {
            return Err(Error::LogCrcMismatch);
        }

        buf.drain(0..8); // Remove header

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_and_read() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let mut log = DataLog::open(f.path()).unwrap();

        let data1 = vec![1; 10];
        let _ = log.append(&data1).unwrap();
        let data2 = vec![2; 100000];
        let k2 = log.append(&data2).unwrap();

        let read_data = log.read(k2).unwrap();
        assert_eq!(read_data, data2);
    }
}
