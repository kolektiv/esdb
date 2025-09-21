use std::{
    error::Error,
    time::Instant,
};

use rocksdb::{
    ColumnFamilyDescriptor,
    MultiThreaded,
    OptimisticTransactionDB,
    OptimisticTransactionOptions,
    Options,
    TransactionDBOptions,
    WriteOptions,
};

type DB = OptimisticTransactionDB<MultiThreaded>;

pub fn main() -> Result<(), Box<dyn Error>> {
    let path = "./data/rocksdb/transactions";

    let cf_options = Options::default();
    let cf = ColumnFamilyDescriptor::new("transactions", cf_options);

    let mut options = Options::default();
    options.create_missing_column_families(true);
    options.create_if_missing(true);

    let mut txn_db_options = TransactionDBOptions::default();
    txn_db_options.set_max_num_locks(256i64);

    let db = DB::open_cf_descriptors(&options, path, [cf])?;

    let cf = db
        .cf_handle("transactions")
        .expect("column family not found");

    // Transactions Testing

    let write_options = WriteOptions::new();
    let opt_txn_options = OptimisticTransactionOptions::new();

    let start = Instant::now();

    let txn_a = db.transaction_opt(&write_options, &opt_txn_options);
    // let txn_b = db.transaction_opt(&write_options, &otxn_options);

    txn_a.put_cf(&cf, "key_a", "value_a")?;
    txn_a.put_cf(&cf, "key_b", "value_b")?;
    txn_a.put_cf(&cf, "key_c", "value_c")?;
    txn_a.put_cf(&cf, "key_d", "value_d")?;
    // txn_b.put_cf(&cf, "key_a", "value_b")?;

    if let Err(err) = txn_a.commit() {
        eprintln!("txn_a: {:#?}", err.kind());
    }

    // if let Err(err) = txn_b.commit() {
    //     eprintln!("txn_b: {:#?}", err.kind());
    // }

    let key_a = db.get_cf(&cf, "key_a")?;

    let duration = Instant::now().duration_since(start);

    println!("read key: {key_a:?}");
    println!("took: {duration:?}");

    Ok(())
}
