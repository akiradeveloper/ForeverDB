use rkyv::util::AlignedVec;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::File;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::Path;

mod error;
pub use error::Error;
use error::Result;

mod device;
use device::Device;
mod op;

mod page;
use page::*;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug)]
pub struct Page {
    pub hashes: Vec<u32>, 
    pub kv_pairs: Vec<(Vec<u8>, Vec<u8>)>,
    pub overflow_id: Option<u64>,
}

impl Page {
    pub fn push(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.hashes.push(xxhash_rust::xxh32::xxh32(&key, 0));
        self.kv_pairs.push((key, value));
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        let h2 = xxhash_rust::xxh32::xxh32(key, 0);
        for (i, h1) in self.hashes.iter().enumerate() {
            if *h1 == h2 {
                if self.kv_pairs[i].0.as_slice() == key {
                    return true;
                }
            }
        }

        false
    }
}

fn calc_max_kv_per_page(ksize: usize, vsize: usize) -> u8 {
    for i in 0..=255 {
        let mut page = Page {
            hashes: Vec::new(),
            kv_pairs: Vec::new(),
            overflow_id: Some(1),
        };
        for j in 0..i {
            page.push(vec![j; ksize], vec![j; vsize]);
        }

        let buf = encode_page(&page);
        if buf.len() > 4088 {
            assert!(i > 2);
            return i - 1;
        }
    }

    255
}

enum PageId {
    Main(u64),
    Overflow(u64),
}

pub struct ForeverHash {
    main_pages: Device,
    main_base_level: u8,
    next_split_main_page_id: u64,

    overflow_pages: Device,
    next_overflow_id: u64,

    n_items: u64,
    max_kv_per_page: Option<u8>,
}

impl ForeverHash {
    pub fn new(main_page_file: &Path, overflow_page_file: &Path) -> Result<Self> {
        let main_page_file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(main_page_file)?;

        let main_pages = Device::new(main_page_file);

        match main_pages.read_page(0)? {
            Some(_) => {}
            None => {
                // Since the pages are not initialized, insert two empty pages.
                main_pages.write_page(
                    0,
                    Page {
                        hashes: Vec::new(),
                        kv_pairs: Vec::new(),
                        overflow_id: None,
                    },
                )?;
                main_pages.write_page(
                    1,
                    Page {
                        hashes: Vec::new(),
                        kv_pairs: Vec::new(),
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

        let overflow_pages = Device::new(overflow_page_file);

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
    #[cfg(not(feature = "versatile"))]
    fn hash(&self, key: &[u8]) -> u64 {
        let a: [u8; 8] = key[0..8].try_into().ok().unwrap();
        u64::from_le_bytes(a)
    }

    #[cfg(feature = "versatile")]
    fn hash(&self, key: &[u8]) -> u64 {
        xxhash_rust::xxh3::xxh3_64(key)
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
        let max_items = n_main_pages * self.max_kv_per_page.unwrap() as u64;
        self.n_items as f64 / max_items as f64
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
