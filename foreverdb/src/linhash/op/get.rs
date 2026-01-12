use super::*;

pub struct Get<'a> {
    pub db: &'a LinHash,
}

impl Get<'_> {
    pub fn exec(self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let b = self.db.calc_main_page_id(key);
        let mut page = self.db.main_pages.read_page(b)?.unwrap();

        loop {
            if let Some(v) = page.kv_pairs.get(key) {
                return Ok(Some(v.clone()));
            }

            match page.overflow_id {
                Some(id) => {
                    page = self.db.overflow_pages.read_page(id)?.unwrap();
                }
                None => {
                    return Ok(None);
                }
            }
        }
    }
}
