use foreverdb::*;
use clap::Parser;
use rand::Rng;
use std::collections::HashSet;

#[derive(Parser)]
struct CommandArgs {
    #[arg(long, default_value_t = 1024)]
    datasize: u32,
    #[arg(long)]
    warmup: u64,
}

fn main() {
    let args = CommandArgs::parse();

    let data_log = {
        let file = tempfile::NamedTempFile::new().unwrap();
        DataLog::open(file.path()).unwrap()
    };

    let db_index = {
        let file = tempfile::NamedTempFile::new().unwrap();
        DBIndex::open(file.path()).unwrap()
    };

    let mut db = ForeverDB::new(data_log, db_index);

    let mut keys = HashSet::new();

    for _ in 0..args.warmup {
        let key = random(64); // 256 bits key
        let data = random(args.datasize as usize);
        db.insert(&key, &data).unwrap();
        keys.insert(key);
    }

    let mut sum_time = std::time::Duration::ZERO;
    let mut cnt = 0;

    let t = std::time::Instant::now();
    while t.elapsed() < std::time::Duration::from_secs(10) {
        let timer = std::time::Instant::now();
        for k in &keys {
            let _ = db.get(k).unwrap();
        }
        let elapsed = timer.elapsed();
        sum_time += elapsed;
        cnt += 1;
    }

    let latency = sum_time / cnt;
    eprintln!("Latency: {:?}", latency);
}

fn random(size: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    (0..size).map(|_| rng.random()).collect()
}