use std::error::Error;

use esdb::{
    Store,
    Stream,
};
use fjall::PartitionCreateOptions;

static PATH: &str = "./data/esdb/experiments";

pub fn main() -> Result<(), Box<dyn Error>> {
    let data = Store::new(PATH)?;

    {
        let mut batch = data.batch();
        let mut stream = Stream::new(&data)?;

        stream.append(&mut batch, vec![
            (
                "hello world!".bytes().collect(),
                ("type:a", 0).into(),
                vec!["tag:a".into(), "tag:b".into()],
            ),
            ("oh, no!".bytes().collect(), ("type:b", 0).into(), vec![
                "tag:a".into(),
                "tag:c".into(),
            ]),
            (
                "goodbye world...".bytes().collect(),
                ("type:a", 1).into(),
                vec!["tag:a".into(), "tag:d".into()],
            ),
        ])?;

        batch.commit()?;
    }

    for partition in data.as_ref().list_partitions() {
        let partition_options = PartitionCreateOptions::default();
        let partition = data
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
