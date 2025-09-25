#![feature(slice_as_array)]

use std::{
    error::Error,
    time::Instant,
};

use esdb_examples::EphemeralStorageEvent;
use fjall::{
    Config,
    Keyspace,
    PartitionCreateOptions,
};

pub fn main() -> Result<(), Box<dyn Error>> {
    let path = "./data/fjall/capacity";

    let config = Config::new(path);
    let keyspace = Keyspace::open(config)?;
    let seq = keyspace.open_partition("capacity", PartitionCreateOptions::default())?;

    // Capacity Testing

    let event = EphemeralStorageEvent {
        data: "some event data here".bytes().collect(),
        tag_values: Vec::from_iter(["tag_a".into(), "tag_b".into()]),
        type_id: 0,
    };

    let mut buffer = Vec::new();
    event.encode(&mut buffer)?;

    let start = Instant::now();

    // for i in 0u64..2u64.pow(20) {
    //     seq.insert(i.to_be_bytes(), &buffer)?;
    // }

    let mut max = 0u64;
    let mut cap = 0u64;

    for event in seq.iter() {
        let (index, _) = event?;
        let index = index.as_array().expect("invalid key");
        let index = u64::from_be_bytes(*index);

        if index > max {
            max = index;
        }

        cap += 1;
    }

    // for i in 0u64..2u64.pow(20) {
    //     let value = seq.get(i.to_be_bytes())?;

    //     if let Some(value) = value {
    //         max += value.len() as u64;
    //     }

    //     cap += 1;
    // }

    // if let Some((key, _)) = seq.last_key_value()? {
    //     let index = key.as_array().expect("invalid key");
    //     let index = u64::from_be_bytes(*index);

    //     println!("last index: {index}");
    // }

    let duration = Instant::now().duration_since(start);

    println!("max: {max} ({cap})");
    println!("took: {duration:?}");

    Ok(())
}
