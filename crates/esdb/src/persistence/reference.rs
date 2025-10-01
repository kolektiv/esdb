use std::error::Error;

use bytes::BufMut;
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
        HashedEvent,
        HashedTag,
    },
};

// =================================================================================================
// Reference
// =================================================================================================

// Configuration

static REFERENCE_PARTITION_NAME: &str = "reference";

static PREFIX_LEN: usize = size_of::<u8>();

// -------------------------------------------------------------------------------------------------

// Reference

#[derive(Debug)]
pub struct Reference {
    descriptor: DescriptorReference,
    tag: TagReference,
}

impl Reference {
    pub fn new(keyspace: &Keyspace) -> Result<Self, Box<dyn Error>> {
        let reference_name = REFERENCE_PARTITION_NAME;
        let reference_options = PartitionCreateOptions::default();
        let reference = keyspace.open_partition(reference_name, reference_options)?;

        let descriptor = DescriptorReference::new(&reference);
        let tag = TagReference::new(&reference);

        Ok(Self { descriptor, tag })
    }
}

impl Reference {
    pub fn insert(&self, batch: &mut Batch, _position: Position, event: &HashedEvent) {
        self.descriptor.insert(batch, &event.descriptor);
        self.tag.insert(batch, &event.tags);
    }
}

// -------------------------------------------------------------------------------------------------
// Descriptor
// -------------------------------------------------------------------------------------------------

// Configuration

static DESCRIPTOR_HASH_LEN: usize = size_of::<u64>();

// -------------------------------------------------------------------------------------------------

// Reference

#[derive(Debug)]
pub struct DescriptorReference {
    lookup: DescriptorLookupReference,
}

impl DescriptorReference {
    fn new(reference: &PartitionHandle) -> Self {
        let lookup = DescriptorLookupReference::new(reference.clone());

        Self { lookup }
    }
}

impl DescriptorReference {
    fn insert(&self, batch: &mut Batch, descriptor: &HashedDescriptor) {
        self.lookup.insert(batch, descriptor);
    }
}

// -------------------------------------------------------------------------------------------------

// Lookup Reference

// Configuration

static DESCRIPTOR_LOOKUP_REFERENCE_KEY: u8 = 0;
static DESCRIPTOR_LOOKUP_REFERENCE_KEY_LEN: usize = PREFIX_LEN + DESCRIPTOR_HASH_LEN;

// Reference

#[derive(Debug)]
pub struct DescriptorLookupReference {
    #[debug("PartitionHandle(\"{}\")", reference.name)]
    reference: PartitionHandle,
}

impl DescriptorLookupReference {
    fn new(reference: PartitionHandle) -> Self {
        Self { reference }
    }
}

impl DescriptorLookupReference {
    fn insert(&self, batch: &mut Batch, descriptor: &HashedDescriptor) {
        let mut key = [0u8; DESCRIPTOR_LOOKUP_REFERENCE_KEY_LEN];
        let mut value = Vec::new();

        {
            let mut key = &mut key[..];

            key.put_u8(DESCRIPTOR_LOOKUP_REFERENCE_KEY);
            key.put_u64(descriptor.hashed().value());

            value.put_u8(descriptor.inner().version().value());
            value.put_slice(descriptor.inner().identifier().value().as_bytes());
        }

        batch.insert(&self.reference, key, value);
    }
}

// -------------------------------------------------------------------------------------------------
// Tag
// -------------------------------------------------------------------------------------------------

// Configuration

static TAG_HASH_LEN: usize = size_of::<u64>();

// -------------------------------------------------------------------------------------------------

// Reference

#[derive(Debug)]
pub struct TagReference {
    lookup: TagLookupReference,
}

impl TagReference {
    fn new(reference: &PartitionHandle) -> Self {
        let lookup = TagLookupReference::new(reference.clone());

        Self { lookup }
    }
}

impl TagReference {
    fn insert(&self, batch: &mut Batch, tags: &[HashedTag]) {
        self.lookup.insert(batch, tags);
    }
}

// -------------------------------------------------------------------------------------------------

// Lookup Reference

// Configuration

static TAG_LOOKUP_REFERENCE_KEY: u8 = 1;
static TAG_LOOKUP_REFERENCE_KEY_LEN: usize = PREFIX_LEN + TAG_HASH_LEN;

// Reference

#[derive(Debug)]
pub struct TagLookupReference {
    #[debug("PartitionHandle(\"{}\")", reference.name)]
    reference: PartitionHandle,
}

impl TagLookupReference {
    fn new(reference: PartitionHandle) -> Self {
        Self { reference }
    }
}

impl TagLookupReference {
    fn insert(&self, batch: &mut Batch, tags: &[HashedTag]) {
        let mut key = [0u8; TAG_LOOKUP_REFERENCE_KEY_LEN];

        for tag in tags {
            {
                let mut key = &mut key[..];

                key.put_u8(TAG_LOOKUP_REFERENCE_KEY);
                key.put_u64(tag.hashed());
            }

            let value = tag.inner().value().as_bytes();

            batch.insert(&self.reference, key, value);
        }
    }
}
