use std::error::Error;

use esdb_examples::EphemeralStorageEvent;
use fjall::{
    Config,
    Keyspace,
    PartitionCreateOptions,
};

pub fn main() -> Result<(), Box<dyn Error>> {
    let path = "./data/fjall/sequence";

    let config = Config::new(path);
    let keyspace = Keyspace::open(config)?;
    let seq = keyspace.open_partition("sequence", PartitionCreateOptions::default())?;
    let seq_meta =
        keyspace.open_partition("sequence_metadata", PartitionCreateOptions::default())?;

    // Sequence Testing

    let mut head = 0u64;

    let event_a = EphemeralStorageEvent {
        data: "event_a".bytes().collect(),
        tag_values: Vec::from_iter(["tag_a".into(), "tag_b".into()]),
        type_id: 0,
    };

    let event_b = EphemeralStorageEvent {
        data: "event_b".bytes().collect(),
        tag_values: Vec::from_iter(["tag_a".into(), "tag_b".into()]),
        type_id: 1,
    };

    let mut buffer = Vec::new();

    event_a.encode(&mut buffer)?;
    seq.insert(head.to_be_bytes(), &buffer)?;
    seq_meta.insert(0u8.to_be_bytes(), head.to_be_bytes())?;

    buffer.clear();
    head += 1;

    event_b.encode(&mut buffer)?;
    seq.insert(head.to_be_bytes(), &buffer)?;
    seq_meta.insert(0u8.to_be_bytes(), head.to_be_bytes())?;

    for data in seq.iter() {
        let (k, v) = data?;

        println!("k: {k:?}");
        println!("v: {v:?}");
    }

    Ok(())
}
