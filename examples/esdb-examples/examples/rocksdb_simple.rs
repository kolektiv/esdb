use std::error::Error;

use rocksdb::{
    DB,
    Options,
};

pub fn main() -> Result<(), Box<dyn Error>> {
    {
        let db = DB::open_default("./data/rocksdb/test")?;
        let () = db.put(b"key_a", b"value_a")?;

        let key_a = db.get(b"key_a")?;

        println!("read key: {key_a:?}");
    }

    let () = DB::destroy(&Options::default(), "./data/rocksdb/test")?;

    Ok(())
}
