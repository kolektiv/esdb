use std::error::Error;

use fjall::{
    Config,
    Keyspace,
    PartitionCreateOptions,
};

pub fn main() -> Result<(), Box<dyn Error>> {
    let keyspace = Keyspace::open(Config::new("./data/fjall/simple"))?;
    let partition = keyspace.open_partition("simple", PartitionCreateOptions::default())?;

    let () = partition.insert(b"key_a", b"value_a")?;
    let key_a = partition.get(b"key_a")?;

    println!("read key: {key_a:?}");

    Ok(())
}
