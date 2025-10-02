use std::error::Error;

use bytes::BufMut as _;
use fjall::{
    PartitionCreateOptions,
    PartitionHandle,
};

use crate::{
    model::Position,
    persistence::{
        Database,
        HashedDescriptor,
        HashedEvent,
        HashedTag,
        POSITION_LEN,
        WriteContext,
    },
};

// =================================================================================================
// Index
// =================================================================================================

static ID_LEN: usize = size_of::<u8>();
static INDEX_PARTITION_NAME: &str = "index";

// Partitions

pub fn partition(database: &Database) -> Result<PartitionHandle, Box<dyn Error>> {
    let name = INDEX_PARTITION_NAME;
    let options = PartitionCreateOptions::default();
    let partition = database.as_ref().open_partition(name, options)?;

    Ok(partition)
}

// Partition Insertion

pub fn insert(ctx: &mut WriteContext<'_>, position: Position, event: &HashedEvent) {
    insert_descriptor(ctx, position, &event.descriptor);
    insert_tags(ctx, position, &event.tags);
}

// -------------------------------------------------------------------------------------------------

// Descriptor

static DESCRIPTOR_HASH_LEN: usize = size_of::<u64>();

// Descriptor Insertion

fn insert_descriptor(
    ctx: &mut WriteContext<'_>,
    position: Position,
    descriptor: &HashedDescriptor,
) {
    insert_descriptor_forward(ctx, position, descriptor);
}

// -------------------------------------------------------------------------------------------------

// Descriptor Forward

static DESCRIPTOR_FORWARD_INDEX_ID: u8 = 0;
static DESCRIPTOR_FORWARD_INDEX_KEY_LEN: usize = ID_LEN + DESCRIPTOR_HASH_LEN + POSITION_LEN;
// static DESCRIPTOR_FORWARD_INDEX_PREFIX_LEN: usize = ID_LEN +
// DESCRIPTOR_HASH_LEN;

// Descriptor Forward Insertion

fn insert_descriptor_forward(
    ctx: &mut WriteContext<'_>,
    position: Position,
    descriptor: &HashedDescriptor,
) {
    let mut key = [0u8; DESCRIPTOR_FORWARD_INDEX_KEY_LEN];
    let value = descriptor.as_ref().version().value().to_be_bytes();

    write_descriptor_forward_key(&mut key, position, descriptor);

    ctx.batch.insert(&ctx.partitions.index, key, value);
}

fn write_descriptor_forward_key(
    key: &mut [u8; DESCRIPTOR_FORWARD_INDEX_KEY_LEN],
    position: Position,
    descriptor: &HashedDescriptor,
) {
    let mut key = &mut key[..];

    let index_id = DESCRIPTOR_FORWARD_INDEX_ID;
    let descriptor_identifier = descriptor.hash();
    let position = position.value();

    key.put_u8(index_id);
    key.put_u64(descriptor_identifier);
    key.put_u64(position);
}

// #[must_use]
// pub fn view(&self, descriptor: HashedDescriptorSpecifier) ->
// DescriptorForwardIndexView {     DescriptorForwardIndexView::new(self.index.
// clone(), descriptor) }

// // View
// #[derive(Debug)]
// pub struct DescriptorForwardIndexView {
//     #[debug("PartitionHandle(\"{}\")", index.name)]
//     index: PartitionHandle,
//     specifier: HashedDescriptorSpecifier,
// }

// impl DescriptorForwardIndexView {
//     fn new(index: PartitionHandle, specifier: HashedDescriptorSpecifier) ->
// Self {         Self { index, specifier }
//     }
// }

// impl IntoIterator for DescriptorForwardIndexView {
//     type IntoIter = DescriptorForwardIndexIterator;
//     type Item = u64;

//     fn into_iter(self) -> Self::IntoIter {
//         let mut prefix = [0u8; DESCRIPTOR_FORWARD_INDEX_PREFIX_LEN];

//         get_descriptor_forward_prefix(&mut prefix, &self.specifier);

//         let version_range = self.specifier.as_ref().range();
//         let version_bounds = version_range
//             .as_ref()
//             .map_or((u8::MIN, u8::MAX), |r| (r.start.value(),
// r.end.value()));

//         let version_min = version_bounds.0;
//         let version_max = version_bounds.1;
//         let version_filter = version_min > u8::MIN || version_max < u8::MAX;

//         let iterator = Box::new(self.index.prefix(prefix).filter_map(move
// |kv| {             let (k, v) = kv.expect("invalid key/value during
// iteration");

//             if version_filter {
//                 let version = v.as_ref().get_u8();

//                 if !(version_min..version_max).contains(&version) {
//                     return None;
//                 }
//             }

//             let mut k = &k[..];

//             k.advance(ID_LEN + DESCRIPTOR_HASH_LEN);

//             let position = k.get_u64();

//             Some(position)
//         }));

//         DescriptorForwardIndexIterator::new(iterator)
//     }
// }

// fn get_descriptor_forward_prefix(
//     prefix: &mut [u8; DESCRIPTOR_FORWARD_INDEX_PREFIX_LEN],
//     specifier: &HashedDescriptorSpecifier,
// ) {
//     let mut prefix = &mut prefix[..];

//     let index_id = DESCRIPTOR_FORWARD_INDEX_ID;
//     let descriptor_identifier = specifier.hash();

//     prefix.put_u8(index_id);
//     prefix.put_u64(descriptor_identifier);
// }

// // Iterator

// #[derive(Debug)]
// pub struct DescriptorForwardIndexIterator {
//     #[debug(skip)]
//     iterator: Box<dyn Iterator<Item = u64>>,
// }

// impl DescriptorForwardIndexIterator {
//     fn new(iterator: Box<dyn Iterator<Item = u64>>) -> Self {
//         Self { iterator }
//     }
// }

// impl Iterator for DescriptorForwardIndexIterator {
//     type Item = u64;

//     fn next(&mut self) -> Option<Self::Item> {
//         self.iterator.next()
//     }
// }

// -------------------------------------------------------------------------------------------------

// Tag

static TAG_HASH_LEN: usize = size_of::<u64>();

// Tag Insertion

fn insert_tags(ctx: &mut WriteContext<'_>, position: Position, tags: &[HashedTag]) {
    insert_tag_forward(ctx, position, tags);
}

// -------------------------------------------------------------------------------------------------

// Tag Forward

static TAG_FORWARD_INDEX_ID: u8 = 1;
static TAG_FORWARD_INDEX_KEY_LEN: usize = ID_LEN + TAG_HASH_LEN + POSITION_LEN;

// Tag Forward Insertion

fn insert_tag_forward(ctx: &mut WriteContext<'_>, position: Position, tags: &[HashedTag]) {
    let mut key = [0u8; TAG_FORWARD_INDEX_KEY_LEN];

    for tag in tags {
        write_tag_forward_key(&mut key, position, tag);

        ctx.batch.insert(&ctx.partitions.index, key, []);
    }
}

fn write_tag_forward_key(
    key: &mut [u8; TAG_FORWARD_INDEX_KEY_LEN],
    position: Position,
    tag: &HashedTag,
) {
    let mut key = &mut key[..];

    let index_id = TAG_FORWARD_INDEX_ID;
    let tag = tag.hash();
    let position = position.value();

    key.put_u8(index_id);
    key.put_u64(tag);
    key.put_u64(position);
}
