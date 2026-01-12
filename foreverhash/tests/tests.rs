use foreverhash::*;

fn vec(i: u32) -> Vec<u8> {
    i.to_le_bytes().to_vec()
}

#[test]
fn test_insert_and_query() {
    let main = tempfile::NamedTempFile::new().unwrap();
    let overflow = tempfile::NamedTempFile::new().unwrap();
    let mut fh = ForeverHash::new(main.path(), overflow.path()).unwrap();

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
