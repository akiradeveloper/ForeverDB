use super::*;

pub struct Device {
    pub f: File,
}

impl Device {
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

        self.f.write_at(&buf, id * 4096).map_err(Error::from)?;

        Ok(())
    }

    pub fn read_page(&self, id: u64) -> Result<Option<Page>> {
        let mut buf = vec![0u8; 4096];
        self.f.read_at(&mut buf, id * 4096).map_err(Error::from)?;
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

    pub fn sync(&self) -> Result<()> {
        self.f.sync_all().map_err(Error::from)?;
        Ok(())
    }
}
