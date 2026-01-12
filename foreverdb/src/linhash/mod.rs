use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

mod error;
pub use error::Error;
use error::Result;

mod device;
use device::Device;
mod op;

enum PageId {
    Main(u64),
    Overflow(u64),
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug)]
pub struct Page {
    pub kv_pairs: HashMap<Vec<u8>, Vec<u8>>,
    pub overflow_id: Option<u64>,
}

fn encode_page(page: &Page) -> Vec<u8> {
    rkyv::to_bytes::<rkyv::rancor::Error>(page)
        .unwrap()
        .to_vec()
}

fn decode_page(buf: &[u8]) -> Result<Page> {
    let page = rkyv::from_bytes::<Page, rkyv::rancor::Error>(buf)?;
    Ok(page)
}

fn calc_max_kv_per_page(ksize: usize, vsize: usize) -> u8 {
    for i in 0..=255 {
        let mut page = Page {
            kv_pairs: HashMap::new(),
            overflow_id: Some(1),
        };
        for j in 0..i {
            page.kv_pairs.insert(vec![j; ksize], vec![j; vsize]);
        }

        let buf = encode_page(&page);
        if buf.len() > 4088 {
            assert!(i > 2);
            return i - 1;
        }
    }

    255
}

pub struct LinHash {
    main_pages: Device,
    main_base_level: u8,
    next_split_main_page_id: u64,

    overflow_pages: Device,
    next_overflow_id: u64,

    n_items: u64,
    max_kv_per_page: Option<u8>,
}

impl LinHash {
    pub fn new(main_page_file: &Path, overflow_page_file: &Path) -> Result<Self> {
        let main_page_file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(main_page_file)?;

        let main_pages = Device { f: main_page_file };

        match main_pages.read_page(0)? {
            Some(_) => {}
            None => {
                // Since the pages are not initialized, insert two empty pages.
                main_pages.write_page(
                    0,
                    Page {
                        kv_pairs: HashMap::new(),
                        overflow_id: None,
                    },
                )?;
                main_pages.write_page(
                    1,
                    Page {
                        kv_pairs: HashMap::new(),
                        overflow_id: None,
                    },
                )?;
            }
        }

        let overflow_page_file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(overflow_page_file)?;

        let overflow_pages = Device {
            f: overflow_page_file,
        };

        Ok(Self {
            main_pages,
            main_base_level: 1,
            next_split_main_page_id: 0,

            overflow_pages,
            next_overflow_id: 0,

            max_kv_per_page: None,
            n_items: 0,
        })
    }

    // The key must be at least 64 bits.
    fn hash(&self, key: &[u8]) -> u64 {
        let a: [u8; 8] = key[0..8].try_into().ok().unwrap();
        u64::from_le_bytes(a)
    }

    fn calc_main_page_id(&self, key: &[u8]) -> u64 {
        let hash = self.hash(key);

        let b = hash & ((1 << self.main_base_level) - 1);
        if b < self.next_split_main_page_id {
            hash & ((1 << (self.main_base_level + 1)) - 1)
        } else {
            b
        }
    }

    fn load_factor(&self) -> f64 {
        let n_main_pages = (1 << self.main_base_level) + self.next_split_main_page_id;
        self.n_items as f64 / (n_main_pages as f64) * (self.max_kv_per_page.unwrap() as f64)
    }

    pub fn len(&self) -> u64 {
        self.n_items
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        op::Get { db: self }.exec(key)
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        op::Insert { db: self }.exec(key, value)?;

        if self.load_factor() > 0.8 {
            op::Split { db: self }.exec().ok();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vec(i: u64) -> Vec<u8> {
        i.to_le_bytes().to_vec()
    }

    #[test]
    fn test_insert_and_query() {
        let main = tempfile::NamedTempFile::new().unwrap();
        let overflow = tempfile::NamedTempFile::new().unwrap();
        let mut fh = LinHash::new(main.path(), overflow.path()).unwrap();

        let n = 10000;
        let range = 0..n;

        for i in range.clone() {
            fh.insert(vec(i), vec(i)).unwrap();
        }

        assert_eq!(fh.len(), n as u64);

        for i in range {
            let v = fh.get(&vec(i)).unwrap().unwrap();
            assert_eq!(v, vec(i));
        }
    }
}
