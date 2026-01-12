use super::*;

pub struct Insert<'a> {
    pub db: &'a mut LinHash,
}

impl Insert<'_> {
    pub fn exec(self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // The `max_kv_per_page` is a fixed value so the size of key and value must be fixed.
        if self.db.max_kv_per_page.is_none() {
            self.db.max_kv_per_page = Some(calc_max_kv_per_page(key.len(), value.len()));
        }

        let b = self.db.calc_main_page_id(&key);

        let mut tail_page = (PageId::Main(b), self.db.main_pages.read_page(b)?.unwrap());

        // Get the tail-page.
        loop {
            if let Some(overflow_id) = tail_page.1.overflow_id {
                tail_page = (
                    PageId::Overflow(overflow_id),
                    self.db.overflow_pages.read_page(overflow_id)?.unwrap(),
                );
            } else {
                break;
            }
        }

        if tail_page.1.kv_pairs.contains_key(&key) {
            return Err(Error::KeyAlreadyExists);
        }

        if tail_page.1.kv_pairs.len() < self.db.max_kv_per_page.unwrap() as usize {
            // If there is space in the tail-page, insert directly.
            tail_page.1.kv_pairs.insert(key, value);
            match tail_page.0 {
                PageId::Main(b) => self.db.main_pages.write_page(b, tail_page.1)?,
                PageId::Overflow(id) => self.db.overflow_pages.write_page(id, tail_page.1)?,
            }
        } else {
            // If not, allocate a new overflow page.
            let new_overflow_id = self.db.next_overflow_id;
            self.db.next_overflow_id += 1;
            let mut new_page = Page {
                kv_pairs: HashMap::new(),
                overflow_id: None,
            };
            new_page.kv_pairs.insert(key, value);
            self.db
                .overflow_pages
                .write_page(new_overflow_id, new_page)?;

            // Since sync is only happened when we allocate a new overflow page and it is rare,
            // the performance impact is small.
            self.db.overflow_pages.sync()?;

            // After writing the new overflow page, update the old tail page.
            tail_page.1.overflow_id = Some(new_overflow_id);
            match tail_page.0 {
                PageId::Main(b) => {
                    self.db.main_pages.write_page(b, tail_page.1)?;
                }
                PageId::Overflow(id) => {
                    self.db.overflow_pages.write_page(id, tail_page.1)?;
                }
            }
        }

        self.db.n_items += 1;

        Ok(())
    }
}
