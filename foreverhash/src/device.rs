use super::*;

struct IO {
    f: File,
}

impl IO {
    pub fn new(f: File) -> Self {
        Self { f }
    }

    pub fn read(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.f.read_at(buf, offset)?;
        Ok(())
    }

    pub fn write(&self, buf: &[u8], offset: u64) -> Result<()> {
        self.f.write_at(buf, offset)?;
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.f.sync_all()?;
        Ok(())
    }
}

pub struct Device {
    io: IO,
}

impl Device {
    pub fn new(f: File) -> Self {
        Self { io: IO::new(f) }
    }

    pub fn write_page(&self, id: u64, page: Page) -> Result<()> {
        let data = encode_page(&page);
        assert!(data.len() <= 4088);

        let buf = {
            let crc = crc32fast::hash(&data);
            let data_len = data.len() as u32;

            let mut out = Vec::with_capacity(8 + data.len());
            out.extend_from_slice(&crc.to_le_bytes());
            out.extend_from_slice(&data_len.to_le_bytes());
            out.extend_from_slice(&data);
            out
        };

        self.io.write(&buf, id * 4096)?;

        Ok(())
    }

    pub fn read_page(&self, id: u64) -> Result<Option<Page>> {
        let mut buf = vec![0u8; 4096];
        self.io.read(&mut buf, id * 4096)?;

        let stored_crc = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let data_len = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        let data = &buf[8..(8 + data_len)];
        let calc_crc = crc32fast::hash(data);
        assert_eq!(stored_crc, calc_crc);

        match decode_page(data) {
            Ok(page) => Ok(Some(page)),
            Err(_) => Ok(None),
        }
    }

    pub fn read_page_ref(&self, id: u64) -> Result<Option<PageRef>> {
        let mut buf = AlignedVec::with_capacity(4096);
        buf.resize(4096, 0);

        self.io.read(&mut buf, id * 4096)?;

        let stored_crc = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let data_len = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        let data_range = 8..(8 + data_len);
        let calc_crc = crc32fast::hash(&buf[data_range.clone()]);
        assert_eq!(stored_crc, calc_crc);

        let page_ref = PageRef { buf, data_range };

        Ok(Some(page_ref))
    }

    pub fn sync(&self) -> Result<()> {
        self.io.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_page_ref() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let device = Device::new(f.reopen().unwrap());

        let mut page = Page {
            hashes: Vec::new(),
            kv_pairs: Vec::new(),
            overflow_id: None,
        };
        page.push(vec![1; 32], vec![1; 16]);
        page.push(vec![2; 32], vec![2; 16]);

        device.write_page(3, page).unwrap();

        let page_ref = device.read_page_ref(3).unwrap().unwrap();
        assert_eq!(page_ref.get_value(&vec![1; 32]), Some(&vec![1; 16][..]));
        assert_eq!(page_ref.get_value(&vec![2; 32]), Some(&vec![2; 16][..]));
    }
}
