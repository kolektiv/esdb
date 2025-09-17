use std::error::Error;

use rocksdb::{
    ColumnFamilyDescriptor,
    MultiThreaded,
    Options,
    TransactionDB,
    TransactionDBOptions,
};

type DB = TransactionDB<MultiThreaded>;

pub fn main() -> Result<(), Box<dyn Error>> {
    let path = "./data/rocksdb/transactions";

    let cf_options = Options::default();
    let cf = ColumnFamilyDescriptor::new("transactions", cf_options);

    let mut options = Options::default();
    options.create_missing_column_families(true);
    options.create_if_missing(true);

    let mut txn_options = TransactionDBOptions::default();
    txn_options.set_max_num_locks(256i64);

    let db = DB::open_cf_descriptors(&options, &txn_options, path, [cf])?;

    let cf = db
        .cf_handle("transactions")
        .expect("column family not found");

    // Transactions Testing

    let () = db.put_cf(&cf, b"key_a", b"value_a")?;
    let key_a = db.get_cf(&cf, b"key_a")?;

    println!("read key: {key_a:?}");

    Ok(())
}
