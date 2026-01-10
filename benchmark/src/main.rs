use clap::Parser;
use foreverdb::*;
use rand::Rng;
use std::collections::HashSet;

#[derive(Parser, Debug)]
struct CommandArgs {
    #[arg(long, default_value_t = 1024)]
    datasize: u32,
    #[arg(long)]
    warmup: u64,
    #[arg(long, default_value_t = false)]
    mem: bool,
    #[arg(long, default_value_t = false)]
    meta: bool,
}

fn main() {
    let args = CommandArgs::parse();
    dbg!(&args);

    let f1 = tempfile::NamedTempFile::new().unwrap();
    let f2 = tempfile::NamedTempFile::new().unwrap();
    let (data_log_path, db_index_path) = if args.mem {
        (f1.path(), f2.path())
    } else {
        let p1 = std::path::Path::new("data.db");
        let p2 = std::path::Path::new("index.db");

        std::fs::remove_file(p1).ok();
        std::fs::remove_file(p2).ok();

        (p1, p2)
    };

    let data_log = DataLog::open(data_log_path).unwrap();
    let db_index = DBIndex::open(db_index_path).unwrap();

    let mut db = ForeverDB::new(data_log, db_index);

    let mut keys = HashSet::new();

    for _ in 0..args.warmup {
        let key = random(64); // 256 bits key
        let data = random(args.datasize as usize);
        db.insert(&key, &data).unwrap();
        keys.insert(key);
    }

    eprintln!("Warmup done. Starting benchmark...");

    let mut results = vec![];

    let t = std::time::Instant::now();
    while t.elapsed() < std::time::Duration::from_secs(10) {
        let timer = std::time::Instant::now();
        for k in &keys {
            if args.meta {
                let _ = db.exists(k).unwrap();
            } else {
                let _ = db.get(k).unwrap();
            }
        }
        let elapsed = timer.elapsed();
        let latency = elapsed / (keys.len() as u32);
        results.push(latency);
    }

    let mut sum = std::time::Duration::ZERO;
    let n = results.len();
    for r in results {
        sum += r;
    }

    eprintln!("Latency: {:?}", sum / n as u32);
}

fn random(size: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    (0..size).map(|_| rng.random()).collect()
}
