use std::error::Error;

use esdb_examples::EphemeralStorageEvent;
use rocksdb::{
    ColumnFamilyDescriptor,
    DB,
    IteratorMode,
    Options,
};

pub fn main() -> Result<(), Box<dyn Error>> {
    let path = "./data/rocksdb/sequence";

    let cf_options = Options::default();
    let cf = ColumnFamilyDescriptor::new("sequence", cf_options);

    let mut db_options = Options::default();
    db_options.create_missing_column_families(true);
    db_options.create_if_missing(true);

    let db = DB::open_cf_descriptors(&db_options, path, [cf])?;

    let cf = db.cf_handle("sequence").expect("column family not found");

    // Sequence Testing

    let mut head = 0u64;

    let event_a = EphemeralStorageEvent {
        data: "event_a".bytes().collect(),
        tag_values: Vec::from_iter(["tag_a".into(), "tag_b".into()]),
        type_id: 0,
    };

    let event_b = EphemeralStorageEvent {
        data: "event_b".bytes().collect(),
        tag_values: Vec::from_iter(["tag_a".into(), "tag_b".into()]),
        type_id: 1,
    };

    let mut buffer = Vec::new();

    event_a.encode(&mut buffer)?;
    db.put_cf(&cf, head.to_be_bytes(), &buffer)?;
    db.put(0u8.to_be_bytes(), head.to_be_bytes())?;

    buffer.clear();
    head += 1;

    event_b.encode(&mut buffer)?;
    db.put_cf(&cf, head.to_be_bytes(), &buffer)?;
    db.put(0u8.to_be_bytes(), head.to_be_bytes())?;

    for data in db.iterator_cf(&cf, IteratorMode::Start) {
        let (k, v) = data?;

        println!("k: {k:?}");
        println!("v: {v:?}");
    }

    Ok(())
}
