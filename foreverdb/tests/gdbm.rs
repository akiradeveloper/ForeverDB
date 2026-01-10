#[test]
fn accept_gdbm() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let db = gdbm::Gdbm::new(
        file.path(),
        0,
        gdbm::Open::WRCREAT,
        libc::S_IRUSR | libc::S_IWUSR,
    )
    .unwrap();
    let key = vec![1];
    let data = vec![1; 10];

    // If content (2nd param) is vec![10] or &vec![10] or data it fetches a wrong data later (why?).
    // Let's always name a variable and take a ref to get a parameter.
    let stored = db.store(&key, &data, false).unwrap();
    assert!(stored);

    let fetched = db.fetch_data(&key).unwrap();
    assert_eq!(fetched, data);
}
