use std::error::Error;

use esdb_examples::EphemeralStorageEvent;

pub fn main() -> Result<(), Box<dyn Error>> {
    let event = EphemeralStorageEvent {
        data: "hello world".bytes().collect(),
        tag_values: Vec::from_iter(["tag_a".into(), "tag_b".into()]),
        type_id: 258,
    };

    println!("event: {event:#?}");

    let mut encoded = Vec::new();

    event.encode(&mut encoded)?;

    println!("encoded: {encoded:?}");

    let decoded = EphemeralStorageEvent::decode(&encoded[..])?;

    println!("decoded: {decoded:#?}");

    Ok(())
}
