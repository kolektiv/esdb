pub mod data;
pub mod index;
pub mod reference;

use std::{
    error::Error,
    path::Path,
};

use derive_more::Debug;
use fjall::{
    Batch,
    Config,
    Keyspace,
    PartitionHandle,
};
use rapidhash::v3::{
    self,
    RapidSecrets,
};

use crate::model::{
    Descriptor,
    DescriptorSpecifier,
    Event,
    Position,
    Tag,
};

// =================================================================================================
// Persistence
// =================================================================================================

// Configuration

static POSITION_LEN: usize = size_of::<u64>();
static SEED: RapidSecrets = RapidSecrets::seed(0x2811_2017);

// -------------------------------------------------------------------------------------------------

// Context

pub struct ReadContext<'a> {
    pub(crate) partitions: &'a Partitions,
}

pub struct WriteContext<'a> {
    pub(crate) batch: &'a mut Batch,
    pub(crate) partitions: &'a Partitions,
}

// -------------------------------------------------------------------------------------------------

// Database

#[derive(Debug)]
pub struct Database {
    #[debug("Keyspace")]
    keyspace: Keyspace,
}

impl AsRef<Keyspace> for Database {
    fn as_ref(&self) -> &Keyspace {
        &self.keyspace
    }
}

pub fn database<P>(path: P) -> Result<Database, Box<dyn Error>>
where
    P: AsRef<Path>,
{
    let keyspace = Config::new(path).open()?;

    Ok(Database { keyspace })
}

impl Database {
    #[must_use]
    pub fn batch(&self) -> Batch {
        self.keyspace.batch()
    }
}

// -------------------------------------------------------------------------------------------------

// Partitions

#[derive(Debug)]
pub struct Partitions {
    #[debug("PartitionHandle(\"{}\")", data.name)]
    data: PartitionHandle,
    #[debug("PartitionHandle(\"{}\")", index.name)]
    index: PartitionHandle,
    #[debug("PartitionHandle(\"{}\")", reference.name)]
    reference: PartitionHandle,
}

pub fn partitions(database: &Database) -> Result<Partitions, Box<dyn Error>> {
    let data = data::partition(database)?;
    let index = index::partition(database)?;
    let reference = reference::partition(database)?;

    Ok(Partitions {
        data,
        index,
        reference,
    })
}

// -------------------------------------------------------------------------------------------------

// Insertion

pub fn insert(ctx: &mut WriteContext<'_>, position: Position, event: Event) {
    let event = event.into();

    // self.data.insert(batch, position, &event);
    // self.index.insert(batch, position, &event);

    data::insert(ctx, position, &event);
    index::insert(ctx, position, &event);
    reference::insert(ctx, &event);

    // self.reference.insert(batch, position, &event);
}

// -------------------------------------------------------------------------------------------------

// Hashed Event

#[derive(Debug)]
pub struct HashedEvent {
    data: Vec<u8>,
    descriptor: HashedDescriptor,
    tags: Vec<HashedTag>,
}

impl From<Event> for HashedEvent {
    fn from(value: Event) -> Self {
        let data = value.data;
        let descriptor = value.descriptor.into();
        let tags = value.tags.into_iter().map(Into::into).collect();

        Self {
            data,
            descriptor,
            tags,
        }
    }
}

// -------------------------------------------------------------------------------------------------

// Hashed Descriptor

// Descriptor

#[derive(Clone, Debug)]
pub struct HashedDescriptor(u64, Descriptor);

impl HashedDescriptor {
    #[must_use]
    pub fn hash(&self) -> u64 {
        self.0
    }
}

impl AsRef<Descriptor> for HashedDescriptor {
    fn as_ref(&self) -> &Descriptor {
        &self.1
    }
}

impl From<Descriptor> for HashedDescriptor {
    fn from(value: Descriptor) -> Self {
        let bytes = value.identifier().value().as_bytes();
        let hash = v3::rapidhash_v3_seeded(bytes, &SEED);

        Self(hash, value)
    }
}

// Specifier

#[derive(Clone, Debug)]
pub struct HashedDescriptorSpecifier(u64, DescriptorSpecifier);

impl HashedDescriptorSpecifier {
    #[must_use]
    pub fn hash(&self) -> u64 {
        self.0
    }
}

impl AsRef<DescriptorSpecifier> for HashedDescriptorSpecifier {
    fn as_ref(&self) -> &DescriptorSpecifier {
        &self.1
    }
}

impl From<DescriptorSpecifier> for HashedDescriptorSpecifier {
    fn from(value: DescriptorSpecifier) -> Self {
        let bytes = value.identifier().value().as_bytes();
        let hash = v3::rapidhash_v3_seeded(bytes, &SEED);

        Self(hash, value)
    }
}

// -------------------------------------------------------------------------------------------------

// Hashed Tag

// Tag

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HashedTag(u64, Tag);

impl HashedTag {
    #[must_use]
    pub fn hash(&self) -> u64 {
        self.0
    }
}

impl AsRef<Tag> for HashedTag {
    fn as_ref(&self) -> &Tag {
        &self.1
    }
}

impl From<Tag> for HashedTag {
    fn from(value: Tag) -> Self {
        let bytes = value.value().as_bytes();
        let hash = v3::rapidhash_v3_seeded(bytes, &SEED);

        Self(hash, value)
    }
}
