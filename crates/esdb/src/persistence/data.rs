use std::error::Error;

use bytes::Buf as _;
use fjall::{
    PartitionCreateOptions,
    PartitionHandle,
};

use crate::{
    model::Position,
    persistence::{
        Database,
        HashedEvent,
        Read,
        Write,
    },
};

// =================================================================================================
// Data
// =================================================================================================

static PARTITION_NAME: &str = "data";

// Partition

pub fn partition(database: &Database) -> Result<PartitionHandle, Box<dyn Error>> {
    Ok(database
        .as_ref()
        .open_partition(PARTITION_NAME, PartitionCreateOptions::default())?)
}

// Properties

pub fn is_empty(read: &Read<'_>) -> Result<bool, Box<dyn Error>> {
    len(read).map(|len| len == 0)
}

pub fn len(read: &Read<'_>) -> Result<u64, Box<dyn Error>> {
    let key_value = read.partitions.data.last_key_value()?;

    if let Some((key, _)) = key_value {
        let key = key.as_ref().get_u64();
        let len = key + 1;

        Ok(len)
    } else {
        Ok(0)
    }
}

// Insertion

pub fn insert(write: &mut Write<'_>, position: Position, event: &HashedEvent) {
    event::insert(write, position, event);
}

// -------------------------------------------------------------------------------------------------

// Event

mod event {
    use bytes::BufMut as _;

    use crate::{
        model::Position,
        persistence::{
            HashedEvent,
            Write,
        },
    };

    // Insertion

    pub fn insert(write: &mut Write<'_>, position: Position, event: &HashedEvent) {
        let key = position.value().to_be_bytes();

        let mut value = Vec::new();

        write_value(&mut value, event);

        write.batch.insert(&write.partitions.data, key, value);
    }

    // Values

    fn write_value(value: &mut Vec<u8>, event: &HashedEvent) {
        let descriptor_identifier = event.descriptor.identifer().hash();
        let descriptor_version = event.descriptor.version().value();
        let tags_len = u8::try_from(event.tags.len()).expect("max tag count exceeded");

        value.put_u64(descriptor_identifier);
        value.put_u8(descriptor_version);
        value.put_u8(tags_len);

        for tag in &event.tags {
            let tag = tag.hash();

            value.put_u64(tag);
        }

        let data = &event.data;

        value.put_slice(data);
    }
}
