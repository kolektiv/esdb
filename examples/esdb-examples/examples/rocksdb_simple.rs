use std::error::Error;

use rocksdb::{
    ColumnFamilyDescriptor,
    DB,
    Options,
};

pub fn main() -> Result<(), Box<dyn Error>> {
    let path = "./data/rocksdb/simple";

    let cf_options = Options::default();
    let cf = ColumnFamilyDescriptor::new("simple", cf_options);

    let mut db_options = Options::default();
    db_options.create_missing_column_families(true);
    db_options.create_if_missing(true);

    let db = DB::open_cf_descriptors(&db_options, path, [cf])?;

    let cf = db.cf_handle("simple").expect("column family not found");
    let () = db.put_cf(&cf, b"key_a", b"value_a")?;
    let key_a = db.get_cf(&cf, b"key_a")?;

    println!("read key: {key_a:?}");

    Ok(())
}
