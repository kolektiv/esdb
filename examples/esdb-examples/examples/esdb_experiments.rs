use std::error::Error;

use esdb::{
    Database,
    Event,
    Stream,
};
use fjall::PartitionCreateOptions;

static PATH: &str = "./data/esdb/experiments";

pub fn main() -> Result<(), Box<dyn Error>> {
    {
        let mut stream = Stream::new(PATH)?;

        stream.append(vec![
            Event::new("hello world!", ("type:a", 0), vec![
                "tag:a".into(),
                "tag:b".into(),
            ]),
            Event::new("oh, no!", ("type:b", 0), vec![
                "tag:a".into(),
                "tag:c".into(),
            ]),
            Event::new("goodbye world...", ("type:a", 1), vec![
                "tag:a".into(),
                "tag:d".into(),
            ]),
        ])?;
    }

    let database = Database::new(PATH)?;

    for partition in database.as_ref().list_partitions() {
        let partition_options = PartitionCreateOptions::default();
        let partition = database
            .as_ref()
            .open_partition(&partition, partition_options)?;

        println!("Partition: {}", partition.name);

        for kv in partition.iter() {
            let (k, v) = kv?;

            println!("{k:?}: {v:?}");
        }
    }

    Ok(())
}
