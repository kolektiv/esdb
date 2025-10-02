pub mod data;
pub mod index;
pub mod reference;

use std::{
    error::Error,
    path::Path,
};

use derive_more::Debug;
use fancy_constructor::new;
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

#[derive(new, Debug)]
#[new(vis(pub(crate)))]
pub struct ReadContext<'a> {
    partitions: &'a Partitions,
}

#[derive(new, Debug)]
#[new(vis(pub(crate)))]
pub struct WriteContext<'a> {
    #[debug("Batch")]
    batch: &'a mut Batch,
    partitions: &'a Partitions,
}

// -------------------------------------------------------------------------------------------------

// Database

#[derive(new, Debug)]
#[new(vis())]
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
    Ok(Config::new(path).open().map(Database::new)?)
}

// -------------------------------------------------------------------------------------------------

// Partitions

#[derive(new, Debug)]
#[new(vis())]
pub struct Partitions {
    #[debug("PartitionHandle(\"{}\")", data.name)]
    data: PartitionHandle,
    #[debug("PartitionHandle(\"{}\")", index.name)]
    index: PartitionHandle,
    #[debug("PartitionHandle(\"{}\")", reference.name)]
    reference: PartitionHandle,
}

pub fn partitions(database: &Database) -> Result<Partitions, Box<dyn Error>> {
    Ok(Partitions::new(
        data::partition(database)?,
        index::partition(database)?,
        reference::partition(database)?,
    ))
}

// -------------------------------------------------------------------------------------------------

// Insertion

pub fn insert(ctx: &mut WriteContext<'_>, position: Position, event: Event) {
    let event = event.into();

    data::insert(ctx, position, &event);
    index::insert(ctx, position, &event);
    reference::insert(ctx, &event);
}

// -------------------------------------------------------------------------------------------------

// Hashed Event

#[derive(new, Debug)]
#[new(vis())]
pub struct HashedEvent {
    #[new(into)]
    data: Vec<u8>,
    #[new(into)]
    descriptor: HashedDescriptor,
    tags: Vec<HashedTag>,
}

impl From<Event> for HashedEvent {
    fn from(event: Event) -> Self {
        Self::new(
            event.data,
            event.descriptor,
            event.tags.into_iter().map(Into::into).collect(),
        )
    }
}

// -------------------------------------------------------------------------------------------------

// Hashed Descriptor

// Descriptor

#[derive(new, Debug)]
#[new(vis())]
pub struct HashedDescriptor(u64, Descriptor);

impl HashedDescriptor {
    fn hash(&self) -> u64 {
        self.0
    }
}

impl AsRef<Descriptor> for HashedDescriptor {
    fn as_ref(&self) -> &Descriptor {
        &self.1
    }
}

impl From<Descriptor> for HashedDescriptor {
    fn from(descriptor: Descriptor) -> Self {
        Self::new(
            v3::rapidhash_v3_seeded(descriptor.identifier().value().as_bytes(), &SEED),
            descriptor,
        )
    }
}

// Specifier

#[derive(new, Clone, Debug)]
#[new(vis())]
pub struct HashedDescriptorSpecifier(u64, DescriptorSpecifier);

impl HashedDescriptorSpecifier {
    fn hash(&self) -> u64 {
        self.0
    }
}

impl AsRef<DescriptorSpecifier> for HashedDescriptorSpecifier {
    fn as_ref(&self) -> &DescriptorSpecifier {
        &self.1
    }
}

impl From<DescriptorSpecifier> for HashedDescriptorSpecifier {
    fn from(descriptor_specifier: DescriptorSpecifier) -> Self {
        Self::new(
            v3::rapidhash_v3_seeded(descriptor_specifier.identifier().value().as_bytes(), &SEED),
            descriptor_specifier,
        )
    }
}

// -------------------------------------------------------------------------------------------------

// Hashed Tag

// Tag

#[derive(new, Debug)]
#[new(vis())]
pub struct HashedTag(u64, Tag);

impl HashedTag {
    fn hash(&self) -> u64 {
        self.0
    }
}

impl AsRef<Tag> for HashedTag {
    fn as_ref(&self) -> &Tag {
        &self.1
    }
}

impl From<Tag> for HashedTag {
    fn from(tag: Tag) -> Self {
        Self::new(v3::rapidhash_v3_seeded(tag.value().as_bytes(), &SEED), tag)
    }
}
