use std::error::Error;

use esdb::stream::Stream;
use fjall::{
    Config,
    PartitionCreateOptions,
};

static PATH: &str = "./data/esdb/experiments";

pub fn main() -> Result<(), Box<dyn Error>> {
    {
        let mut stream = Stream::new(PATH)?;

        stream.append(&[
            ("hello world!".as_bytes(), 234),
            ("oh, something else!".as_bytes(), 67),
            ("goodbye cruel world...".as_bytes(), 234),
        ])?;
    }

    let keyspace = Config::new(PATH).open()?;

    for partition in keyspace.list_partitions() {
        let partition_options = PartitionCreateOptions::default();
        let partition = keyspace.open_partition(&partition, partition_options)?;

        println!("Partition: {}", partition.name);

        for kv in partition.iter() {
            let (k, v) = kv?;

            println!("{k:?}: {v:?}");
        }
    }

    Ok(())
}
