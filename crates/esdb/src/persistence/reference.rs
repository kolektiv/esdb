use std::error::Error;

use bytes::BufMut;
use fjall::{
    PartitionCreateOptions,
    PartitionHandle,
};

use crate::persistence::{
    Database,
    HashedDescriptor,
    HashedEvent,
    HashedTag,
    WriteContext,
};

// =================================================================================================
// Reference
// =================================================================================================

static ID_LEN: usize = size_of::<u8>();
static REFERENCE_PARTITION_NAME: &str = "reference";

// Partition

pub fn partition(database: &Database) -> Result<PartitionHandle, Box<dyn Error>> {
    Ok(database
        .as_ref()
        .open_partition(REFERENCE_PARTITION_NAME, PartitionCreateOptions::default())?)
}

// Partition Insertion

pub fn insert(ctx: &mut WriteContext<'_>, event: &HashedEvent) {
    insert_descriptor(ctx, &event.descriptor);
    insert_tags(ctx, &event.tags);
}

// -------------------------------------------------------------------------------------------------

// Descriptor

static DESCRIPTOR_HASH_LEN: usize = size_of::<u64>();

// Descriptor Insertion

fn insert_descriptor(ctx: &mut WriteContext<'_>, descriptor: &HashedDescriptor) {
    insert_descriptor_lookup(ctx, descriptor);
}

// Lookup

static DESCRIPTOR_LOOKUP_REFERENCE_ID: u8 = 0;
static DESCRIPTOR_LOOKUP_REFERENCE_KEY_LEN: usize = ID_LEN + DESCRIPTOR_HASH_LEN;

fn insert_descriptor_lookup(ctx: &mut WriteContext<'_>, descriptor: &HashedDescriptor) {
    let mut key = [0u8; DESCRIPTOR_LOOKUP_REFERENCE_KEY_LEN];

    let value = descriptor.identifer().value().as_bytes();

    write_descriptor_lookup_key(&mut key, descriptor);

    ctx.batch.insert(&ctx.partitions.reference, key, value);
}

fn write_descriptor_lookup_key(
    key: &mut [u8; DESCRIPTOR_LOOKUP_REFERENCE_KEY_LEN],
    descriptor: &HashedDescriptor,
) {
    let mut key = &mut key[..];

    let reference_id = DESCRIPTOR_LOOKUP_REFERENCE_ID;
    let descriptor_identifier = descriptor.identifer().hash();

    key.put_u8(reference_id);
    key.put_u64(descriptor_identifier);
}

// -------------------------------------------------------------------------------------------------

// Tag

static TAG_HASH_LEN: usize = size_of::<u64>();

// Tag Insertion

fn insert_tags(ctx: &mut WriteContext<'_>, tags: &[HashedTag]) {
    insert_tags_lookup(ctx, tags);
}

// Lookup

static TAG_LOOKUP_REFERENCE_ID: u8 = 1;
static TAG_LOOKUP_REFERENCE_KEY_LEN: usize = ID_LEN + TAG_HASH_LEN;

fn insert_tags_lookup(ctx: &mut WriteContext<'_>, tags: &[HashedTag]) {
    let mut key = [0u8; TAG_LOOKUP_REFERENCE_KEY_LEN];

    for tag in tags {
        write_tag_lookup_key(&mut key, tag);

        let value = tag.value().as_bytes();

        ctx.batch.insert(&ctx.partitions.reference, key, value);
    }
}

fn write_tag_lookup_key(key: &mut [u8; TAG_LOOKUP_REFERENCE_KEY_LEN], tag: &HashedTag) {
    let mut key = &mut key[..];

    let reference_id = TAG_LOOKUP_REFERENCE_ID;
    let tag = tag.hash();

    key.put_u8(reference_id);
    key.put_u64(tag);
}
