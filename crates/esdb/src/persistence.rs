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
};
use rapidhash::v3::{
    self,
    RapidSecrets,
};

use crate::{
    model::{
        Descriptor,
        DescriptorSpecifier,
        Event,
        Position,
        Tag,
    },
    persistence::{
        data::Data,
        index::Index,
        reference::Reference,
    },
};

// =================================================================================================
// Persistence
// =================================================================================================

// Configuration

static POSITION_LEN: usize = size_of::<u64>();

// RapidHash

static SEED: RapidSecrets = RapidSecrets::seed(0x2811_2017);

// -------------------------------------------------------------------------------------------------

// Database

#[derive(Debug)]
pub struct Database {
    #[debug("Keyspace")]
    keyspace: Keyspace,
}

impl Database {
    pub fn new<P>(path: P) -> Result<Self, Box<dyn Error>>
    where
        P: AsRef<Path>,
    {
        let keyspace = Config::new(path).open()?;

        Ok(Self { keyspace })
    }
}

impl Database {
    #[must_use]
    pub fn batch(&self) -> Batch {
        self.keyspace.batch()
    }
}

impl AsRef<Keyspace> for Database {
    fn as_ref(&self) -> &Keyspace {
        &self.keyspace
    }
}

// -------------------------------------------------------------------------------------------------

// Store

#[derive(Debug)]
pub struct Store {
    data: Data,
    index: Index,
    reference: Reference,
}

impl Store {
    pub fn new(store: &Database) -> Result<Self, Box<dyn Error>> {
        let data = Data::new(store.as_ref())?;
        let index = Index::new(store.as_ref())?;
        let reference = Reference::new(store.as_ref())?;

        Ok(Self {
            data,
            index,
            reference,
        })
    }
}

impl Store {
    pub fn insert(&self, batch: &mut Batch, position: Position, event: Event) {
        let event = event.into();

        self.data.insert(batch, position, &event);
        self.index.insert(batch, position, &event);
        self.reference.insert(batch, position, &event);
    }
}

impl AsRef<Data> for Store {
    fn as_ref(&self) -> &Data {
        &self.data
    }
}

impl AsRef<Index> for Store {
    fn as_ref(&self) -> &Index {
        &self.index
    }
}

impl AsRef<Reference> for Store {
    fn as_ref(&self) -> &Reference {
        &self.reference
    }
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
