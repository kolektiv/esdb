use std::error::Error;

use bytes::{
    Buf as _,
    BufMut as _,
};
use fjall::{
    PartitionCreateOptions,
    PartitionHandle,
};

use crate::{
    model::Position,
    persistence::{
        Database,
        HashedEvent,
        ReadContext,
        WriteContext,
    },
};

// =================================================================================================
// Data
// =================================================================================================

static DATA_PARTITION_NAME: &str = "data";

// Partition

pub fn partition(database: &Database) -> Result<PartitionHandle, Box<dyn Error>> {
    Ok(database
        .as_ref()
        .open_partition(DATA_PARTITION_NAME, PartitionCreateOptions::default())?)
}

// Partition Properties

pub fn is_empty(ctx: &ReadContext<'_>) -> Result<bool, Box<dyn Error>> {
    len(ctx).map(|len| len == 0)
}

pub fn len(ctx: &ReadContext<'_>) -> Result<u64, Box<dyn Error>> {
    Ok(match ctx.partitions.data.last_key_value()? {
        Some((key, _)) => key.as_ref().get_u64() + 1,
        None => 0,
    })
}

// Partition Insertion

pub fn insert(ctx: &mut WriteContext<'_>, position: Position, event: &HashedEvent) {
    insert_event(ctx, position, event);
}

// -------------------------------------------------------------------------------------------------

// Event

// Event Insertion

fn insert_event(ctx: &mut WriteContext<'_>, position: Position, event: &HashedEvent) {
    let key = position.value().to_be_bytes();
    let mut value = Vec::new();

    write_event_value(&mut value, event);

    ctx.batch.insert(&ctx.partitions.data, key, value);
}

fn write_event_value(value: &mut Vec<u8>, event: &HashedEvent) {
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
