use std::error::Error;

use bytes::{
    Buf,
    BufMut as _,
    BytesMut,
};
use fjall::{
    Config,
    PartitionCreateOptions,
};

static PATH: &str = "./data/esdb/experiments";

pub fn main() -> Result<(), Box<dyn Error>> {
    let keyspace = Config::new(PATH).open()?;
    let partition = keyspace.open_partition("indices", PartitionCreateOptions::default())?;

    let mut key = BytesMut::with_capacity(9);
    key.put_u64(234);
    key.put_u8(0u8);

    let iterator_a = partition.prefix(&key[..]).map(|kv| {
        let mut k = &kv.expect("iteration error: key/value").0[..];

        k.advance(9);
        k.get_u64()
    });

    for pos in iterator_a {
        println!("pos: {pos:?}");
    }

    Ok(())
}
