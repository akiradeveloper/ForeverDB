use foreverdb::*;

#[test]
fn test_insert_and_get() {
    let log_file = tempfile::NamedTempFile::new().unwrap();
    let main_file = tempfile::NamedTempFile::new().unwrap();
    let overflow_file = tempfile::NamedTempFile::new().unwrap();

    let data_log = DataLog::open(log_file.path()).unwrap();
    let db_index = DBIndex::open(main_file.path(), overflow_file.path()).unwrap();
    let mut db = ForeverDB::new(data_log, db_index);

    let k1 = vec![1; 32];
    let v1 = vec![42; 100];

    let k2 = vec![2; 32];
    let v2 = vec![43; 100];

    db.insert(k1.clone(), v1.clone()).unwrap();
    db.insert(k2.clone(), v2.clone()).unwrap();
    assert!(db.exists(&k1).unwrap());
    assert!(db.exists(&k2).unwrap());

    assert_eq!(db.get(&k1).unwrap().unwrap(), v1);
    assert_eq!(db.get(&k2).unwrap().unwrap(), v2);
}
