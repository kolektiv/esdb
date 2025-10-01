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
        HashedTag,
    },
};

// =================================================================================================
// Index
// =================================================================================================

// Configuration

static INDEX_PARTITION_NAME: &str = "index";

static POSITION_LEN: usize = size_of::<u64>();
static PREFIX_LEN: usize = size_of::<u8>();

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
    pub fn insert(
        &self,
        batch: &mut Batch,
        descriptor: &HashedDescriptor,
        tags: &[HashedTag],
        position: Position,
    ) {
        self.descriptor.insert(batch, descriptor, position);
        self.tag.insert(batch, tags, position);
    }
}

// -------------------------------------------------------------------------------------------------
// Descriptor
// -------------------------------------------------------------------------------------------------

// Configuration

static DESCRIPTOR_HASH_LEN: usize = size_of::<u64>();
static DESCRIPTOR_VERSION_LEN: usize = size_of::<u8>();

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
    pub fn insert(&self, batch: &mut Batch, type_value: &HashedDescriptor, position: Position) {
        self.forward.insert(batch, type_value, position);
    }
}

// -------------------------------------------------------------------------------------------------

// Forward Index

// Configuration

static DESCRIPTOR_FORWARD_INDEX_KEY: u8 = 0;
static DESCRIPTOR_KEY_LEN: usize = PREFIX_LEN + DESCRIPTOR_HASH_LEN + POSITION_LEN;
static DESCRIPTOR_VALUE_LEN: usize = DESCRIPTOR_VERSION_LEN;
static DESCRIPTOR_PREFIX_LEN: usize = PREFIX_LEN + DESCRIPTOR_HASH_LEN;

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
    pub fn insert(&self, batch: &mut Batch, descriptor: &HashedDescriptor, position: Position) {
        let mut key = [0u8; DESCRIPTOR_KEY_LEN];
        let mut value = [0u8; DESCRIPTOR_VALUE_LEN];

        {
            let mut key = &mut key[..];

            key.put_u8(DESCRIPTOR_FORWARD_INDEX_KEY);
            key.put_u64(descriptor.hashed().value());
            key.put_u64(position.value());

            let mut value = &mut value[..];

            value.put_u8(descriptor.inner().version().value());
        }

        batch.insert(&self.index, key, value);
    }

    #[must_use]
    pub fn view(&self, descriptor: HashedDescriptorSpecifier) -> DescriptorForwardIndexView {
        DescriptorForwardIndexView::new(self.index.clone(), descriptor)
    }
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
    type IntoIter = DescriptorIndexForwardIterator;
    type Item = u64;

    fn into_iter(self) -> Self::IntoIter {
        let type_id = self.specifier.hashed().value();

        let mut prefix = [0u8; DESCRIPTOR_PREFIX_LEN];

        {
            let mut prefix = &mut prefix[..];

            prefix.put_u8(DESCRIPTOR_FORWARD_INDEX_KEY);
            prefix.put_u64(type_id);
        }

        let version_bounds = self
            .specifier
            .inner()
            .range()
            .as_ref()
            .map_or((u8::MIN, u8::MAX), |r| (r.start.value(), r.end.value()));

        let version_min = version_bounds.0;
        let version_max = version_bounds.1;
        let version_perform_filter = version_min > u8::MIN || version_max < u8::MAX;

        let iterator = Box::new(self.index.prefix(prefix).filter_map(move |kv| {
            let (k, v) = kv.expect("invalid key/value during iteration");

            if version_perform_filter {
                let mut v = &v[..];

                let version = v.get_u8();

                if !(version_min..version_max).contains(&version) {
                    return None;
                }
            }

            let mut k = &k[..];

            k.advance(PREFIX_LEN + DESCRIPTOR_HASH_LEN);

            let position = k.get_u64();

            Some(position)
        }));

        DescriptorIndexForwardIterator::new(iterator)
    }
}

// Iterator

#[derive(Debug)]
pub struct DescriptorIndexForwardIterator {
    #[debug(skip)]
    iterator: Box<dyn Iterator<Item = u64>>,
}

impl DescriptorIndexForwardIterator {
    fn new(iterator: Box<dyn Iterator<Item = u64>>) -> Self {
        Self { iterator }
    }
}

impl Iterator for DescriptorIndexForwardIterator {
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
    pub fn insert(&self, batch: &mut Batch, tags: &[HashedTag], position: Position) {
        self.forward.insert(batch, tags, position);
    }
}

// -------------------------------------------------------------------------------------------------

// Forward Index

// Configuration

static TAG_FORWARD_INDEX_KEY: u8 = 1;
static TAG_KEY_LEN: usize = PREFIX_LEN + TAG_HASH_LEN + POSITION_LEN;

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
    pub fn insert(&self, batch: &mut Batch, tags: &[HashedTag], position: Position) {
        let mut key = [0u8; TAG_KEY_LEN];

        for tag in tags {
            {
                let mut key = &mut key[..];

                key.put_u8(TAG_FORWARD_INDEX_KEY);
                key.put_u64(tag.hashed());
                key.put_u64(position.value());
            }

            batch.insert(&self.index, key, []);
        }
    }

    // #[must_use]
    // pub fn view(&self, descriptor: HashedDescriptorSpecifier) ->
    // DescriptorForwardIndexView {     DescriptorForwardIndexView::new(self.
    // index.clone(), descriptor) }
}
