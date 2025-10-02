use std::error::Error;

use bytes::{
    Buf as _,
    BufMut as _,
};
use derive_more::Debug;
use fjall::{
    Batch,
    Keyspace,
    PartitionCreateOptions,
    PartitionHandle,
};

use crate::{
    model::Position,
    persistence::{
        HashedDescriptor,
        HashedDescriptorSpecifier,
        HashedEvent,
        HashedTag,
        POSITION_LEN,
    },
};

// =================================================================================================
// Index
// =================================================================================================

// Configuration

static ID_LEN: usize = size_of::<u8>();
static INDEX_PARTITION_NAME: &str = "index";

// -------------------------------------------------------------------------------------------------

// Index

#[derive(Debug)]
pub struct Index {
    descriptor: DescriptorIndex,
    tag: TagIndex,
}

impl Index {
    pub fn new(keyspace: &Keyspace) -> Result<Self, Box<dyn Error>> {
        let index_name = INDEX_PARTITION_NAME;
        let index_options = PartitionCreateOptions::default();
        let index = keyspace.open_partition(index_name, index_options)?;

        let descriptor = DescriptorIndex::new(&index);
        let tag = TagIndex::new(&index);

        Ok(Self { descriptor, tag })
    }
}

impl Index {
    pub fn insert(&self, batch: &mut Batch, position: Position, event: &HashedEvent) {
        self.descriptor.insert(batch, position, &event.descriptor);
        self.tag.insert(batch, position, &event.tags);
    }
}

// -------------------------------------------------------------------------------------------------
// Descriptor
// -------------------------------------------------------------------------------------------------

// Configuration

static DESCRIPTOR_HASH_LEN: usize = size_of::<u64>();

// -------------------------------------------------------------------------------------------------

// Index

#[derive(Debug)]
pub struct DescriptorIndex {
    pub forward: DescriptorForwardIndex,
}

impl DescriptorIndex {
    #[must_use]
    pub fn new(index: &PartitionHandle) -> Self {
        let forward = DescriptorForwardIndex::new(index.clone());

        Self { forward }
    }
}

impl DescriptorIndex {
    pub fn insert(&self, batch: &mut Batch, position: Position, descriptor: &HashedDescriptor) {
        self.forward.insert(batch, position, descriptor);
    }
}

// -------------------------------------------------------------------------------------------------

// Forward Index

// Configuration

static DESCRIPTOR_FORWARD_INDEX_ID: u8 = 0;
static DESCRIPTOR_FORWARD_INDEX_KEY_LEN: usize = ID_LEN + DESCRIPTOR_HASH_LEN + POSITION_LEN;
static DESCRIPTOR_FORWARD_INDEX_PREFIX_LEN: usize = ID_LEN + DESCRIPTOR_HASH_LEN;

// Index

#[derive(Debug)]
pub struct DescriptorForwardIndex {
    #[debug("PartitionHandle(\"{}\")", index.name)]
    index: PartitionHandle,
}

impl DescriptorForwardIndex {
    #[must_use]
    pub fn new(index: PartitionHandle) -> Self {
        Self { index }
    }
}

impl DescriptorForwardIndex {
    pub fn insert(&self, batch: &mut Batch, position: Position, descriptor: &HashedDescriptor) {
        let mut key = [0u8; DESCRIPTOR_FORWARD_INDEX_KEY_LEN];
        let value = descriptor.as_ref().version().value().to_be_bytes();

        get_descriptor_forward_key(&mut key, position, descriptor);

        batch.insert(&self.index, key, value);
    }

    #[must_use]
    pub fn view(&self, descriptor: HashedDescriptorSpecifier) -> DescriptorForwardIndexView {
        DescriptorForwardIndexView::new(self.index.clone(), descriptor)
    }
}

fn get_descriptor_forward_key(
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

// View

#[derive(Debug)]
pub struct DescriptorForwardIndexView {
    #[debug("PartitionHandle(\"{}\")", index.name)]
    index: PartitionHandle,
    specifier: HashedDescriptorSpecifier,
}

impl DescriptorForwardIndexView {
    fn new(index: PartitionHandle, specifier: HashedDescriptorSpecifier) -> Self {
        Self { index, specifier }
    }
}

impl IntoIterator for DescriptorForwardIndexView {
    type IntoIter = DescriptorForwardIndexIterator;
    type Item = u64;

    fn into_iter(self) -> Self::IntoIter {
        let mut prefix = [0u8; DESCRIPTOR_FORWARD_INDEX_PREFIX_LEN];

        get_descriptor_forward_prefix(&mut prefix, &self.specifier);

        let version_range = self.specifier.as_ref().range();
        let version_bounds = version_range
            .as_ref()
            .map_or((u8::MIN, u8::MAX), |r| (r.start.value(), r.end.value()));

        let version_min = version_bounds.0;
        let version_max = version_bounds.1;
        let version_filter = version_min > u8::MIN || version_max < u8::MAX;

        let iterator = Box::new(self.index.prefix(prefix).filter_map(move |kv| {
            let (k, v) = kv.expect("invalid key/value during iteration");

            if version_filter {
                let version = v.as_ref().get_u8();

                if !(version_min..version_max).contains(&version) {
                    return None;
                }
            }

            let mut k = &k[..];

            k.advance(ID_LEN + DESCRIPTOR_HASH_LEN);

            let position = k.get_u64();

            Some(position)
        }));

        DescriptorForwardIndexIterator::new(iterator)
    }
}

fn get_descriptor_forward_prefix(
    prefix: &mut [u8; DESCRIPTOR_FORWARD_INDEX_PREFIX_LEN],
    specifier: &HashedDescriptorSpecifier,
) {
    let mut prefix = &mut prefix[..];

    let index_id = DESCRIPTOR_FORWARD_INDEX_ID;
    let descriptor_identifier = specifier.hash();

    prefix.put_u8(index_id);
    prefix.put_u64(descriptor_identifier);
}

// Iterator

#[derive(Debug)]
pub struct DescriptorForwardIndexIterator {
    #[debug(skip)]
    iterator: Box<dyn Iterator<Item = u64>>,
}

impl DescriptorForwardIndexIterator {
    fn new(iterator: Box<dyn Iterator<Item = u64>>) -> Self {
        Self { iterator }
    }
}

impl Iterator for DescriptorForwardIndexIterator {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next()
    }
}

// -------------------------------------------------------------------------------------------------
// Tag
// -------------------------------------------------------------------------------------------------

// Configuration

static TAG_HASH_LEN: usize = size_of::<u64>();

// -------------------------------------------------------------------------------------------------

// Index

#[derive(Debug)]
pub struct TagIndex {
    pub forward: TagForwardIndex,
}

impl TagIndex {
    #[must_use]
    pub fn new(index: &PartitionHandle) -> Self {
        let forward = TagForwardIndex::new(index.clone());

        Self { forward }
    }
}

impl TagIndex {
    pub fn insert(&self, batch: &mut Batch, position: Position, tags: &[HashedTag]) {
        self.forward.insert(batch, position, tags);
    }
}

// -------------------------------------------------------------------------------------------------

// Forward Index

// Configuration

static TAG_FORWARD_INDEX_ID: u8 = 1;
static TAG_FORWARD_INDEX_KEY_LEN: usize = ID_LEN + TAG_HASH_LEN + POSITION_LEN;

// Index

#[derive(Debug)]
pub struct TagForwardIndex {
    #[debug("PartitionHandle(\"{}\")", index.name)]
    index: PartitionHandle,
}

impl TagForwardIndex {
    #[must_use]
    pub fn new(index: PartitionHandle) -> Self {
        Self { index }
    }
}

impl TagForwardIndex {
    pub fn insert(&self, batch: &mut Batch, position: Position, tags: &[HashedTag]) {
        let mut key = [0u8; TAG_FORWARD_INDEX_KEY_LEN];

        for tag in tags {
            get_tag_forward_key(&mut key, position, tag);

            batch.insert(&self.index, key, []);
        }
    }

    // #[must_use]
    // pub fn view(&self, descriptor: HashedDescriptorSpecifier) ->
    // DescriptorForwardIndexView {     DescriptorForwardIndexView::new(self.
    // index.clone(), descriptor) }
}

fn get_tag_forward_key(
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
