pub mod data;
pub mod index;
pub mod reference;

use std::{
    error::Error,
    ops::{
        Deref,
        Range,
    },
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
    Event,
    Identifier,
    Position,
    Specifier,
    Tag,
    Version,
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
pub struct Read<'a> {
    partitions: &'a Partitions,
}

#[derive(new, Debug)]
#[new(vis(pub(crate)))]
pub struct Write<'a> {
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

pub fn insert(write: &mut Write<'_>, position: Position, event: Event) {
    let event = event.into();

    data::insert(write, position, &event);
    index::insert(write, position, &event);
    reference::insert(write, &event);
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
pub struct HashedDescriptor(HashedIdentifier, Version);

impl HashedDescriptor {
    fn identifer(&self) -> &HashedIdentifier {
        &self.0
    }

    fn version(&self) -> &Version {
        &self.1
    }
}

impl From<Descriptor> for HashedDescriptor {
    fn from(descriptor: Descriptor) -> Self {
        let descriptor = descriptor.take();
        let identifier = descriptor.0.into();
        let version = descriptor.1;

        Self::new(identifier, version)
    }
}

// Identifier

#[derive(new, Debug)]
#[new(vis())]
pub struct HashedIdentifier(u64, Identifier);

impl HashedIdentifier {
    fn hash(&self) -> u64 {
        self.0
    }
}

impl Deref for HashedIdentifier {
    type Target = Identifier;

    fn deref(&self) -> &Self::Target {
        &self.1
    }
}

impl From<Identifier> for HashedIdentifier {
    fn from(descriptor_identifier: Identifier) -> Self {
        Self::new(
            v3::rapidhash_v3_seeded(descriptor_identifier.value().as_bytes(), &SEED),
            descriptor_identifier,
        )
    }
}

// Specifier

#[derive(new, Debug)]
#[new(vis())]
pub struct HashedSpecifier(HashedIdentifier, Option<Range<Version>>);

impl HashedSpecifier {
    fn identifer(&self) -> &HashedIdentifier {
        &self.0
    }

    fn range(&self) -> Option<&Range<Version>> {
        self.1.as_ref()
    }
}

impl From<Specifier> for HashedSpecifier {
    fn from(descriptor_specifier: Specifier) -> Self {
        let descriptor_specifier = descriptor_specifier.take();
        let identifier = descriptor_specifier.0.into();
        let range = descriptor_specifier.1;

        Self::new(identifier, range)
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

impl Deref for HashedTag {
    type Target = Tag;

    fn deref(&self) -> &Self::Target {
        &self.1
    }
}

impl From<Tag> for HashedTag {
    fn from(tag: Tag) -> Self {
        Self::new(v3::rapidhash_v3_seeded(tag.value().as_bytes(), &SEED), tag)
    }
}
