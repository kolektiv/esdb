use std::error::Error;

use rocksdb::{
    DB,
    Options,
};

pub fn main() -> Result<(), Box<dyn Error>> {
    let mut db = DB::open_default("./data/rocksdb/simple")?;
    let () = db.create_cf("simple", &Options::default())?;
    let cf = db.cf_handle("simple").unwrap();

    let () = db.put_cf(&cf, b"key_a", b"value_a")?;
    let key_a = db.get_cf(&cf, b"key_a")?;

    println!("read key: {key_a:?}");

    Ok(())
}
