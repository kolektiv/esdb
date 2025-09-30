use std::error::Error;

use bytes::BufMut;
use esdb::{
    data::Data,
    stream::Stream,
};
use fjall::PartitionCreateOptions;

static PATH: &str = "./data/esdb/experiments";

pub fn main() -> Result<(), Box<dyn Error>> {
    let data = Data::new(PATH)?;

    {
        let mut batch = data.batch();
        let mut stream = Stream::new(&data)?;

        stream.append(&mut batch, &[
            ("hello world!".as_bytes(), &[15, 1008], (234, 0).into()),
            ("oh, no!".as_bytes(), &[25, 1008], (67, 0).into()),
            ("goodbye world...".as_bytes(), &[4, 1678], (234, 1).into()),
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

    let partition = data
        .as_ref()
        .open_partition("indices", PartitionCreateOptions::default())?;

    let mut lower = [0u8; 17];

    {
        let mut lower = &mut lower[..];

        lower.put_u8(0);
        lower.put_u64(234);
        lower.put_u64(0);
    }

    let mut upper = [0u8; 17];

    {
        let mut upper = &mut upper[..];

        upper.put_u8(0);
        upper.put_u64(234);
        upper.put_u64(u64::MAX);
    }

    println!("range: {lower:?}..{upper:?}");

    for kv in partition.range(lower..upper) {
        let (k, v) = kv?;

        println!("k: {k:?}: {v:?}");
    }

    Ok(())
}
