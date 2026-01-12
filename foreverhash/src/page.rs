use super::*;

type ArchivedPage = <Page as rkyv::Archive>::Archived;

pub fn encode_page(page: &Page) -> Vec<u8> {
    rkyv::to_bytes::<rkyv::rancor::Error>(page)
        .unwrap()
        .to_vec()
}

pub fn decode_page(buf: &[u8]) -> Result<Page> {
    let page = rkyv::from_bytes::<Page, rkyv::rancor::Error>(buf)?;
    Ok(page)
}

pub struct PageRef {
    pub buf: AlignedVec,
    pub data_range: Range<usize>,
}

impl PageRef {
    #[inline]
    fn data(&self) -> &[u8] {
        &self.buf[self.data_range.clone()]
    }

    #[inline]
    fn archived(&self) -> &ArchivedPage {
        unsafe { rkyv::access_unchecked::<ArchivedPage>(self.data()) }
    }

    pub fn get_value(&self, key: &[u8]) -> Option<&[u8]> {
        let page = self.archived();

        let h2 = xxhash_rust::xxh32::xxh32(key, 0);
        for (i, h1) in page.hashes.iter().enumerate() {
            if h1.to_native() == h2 {
                let kv = &page.kv_pairs[i];
                if kv.0.as_slice() == key {
                    return Some(kv.1.as_slice());
                }
            }
        }

        None
    }

    pub fn overflow_id(&self) -> Option<u64> {
        self.archived().overflow_id.as_ref().map(|x| x.to_native())
    }
}
