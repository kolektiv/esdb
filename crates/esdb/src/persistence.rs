pub mod data;
pub mod index;
pub mod reference;

use std::{
    error::Error,
    path::Path,
};

use bytes::BufMut;
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
pub struct HashedDescriptor(HashedDescriptorIdentifier, Descriptor);

impl HashedDescriptor {
    pub fn hashed(&self) -> &HashedDescriptorIdentifier {
        &self.0
    }

    pub fn inner(&self) -> &Descriptor {
        &self.1
    }
}

impl From<Descriptor> for HashedDescriptor {
    fn from(value: Descriptor) -> Self {
        let mut bytes = Vec::new();

        {
            bytes.put_slice(value.identifier().value().as_bytes());
            bytes.put_u8(value.version().value());
        }

        let identifier = v3::rapidhash_v3_seeded(&bytes, &SEED).into();

        Self(identifier, value)
    }
}

// Identifier

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct HashedDescriptorIdentifier(u64);

impl HashedDescriptorIdentifier {
    pub fn value(self) -> u64 {
        self.0
    }
}

impl<T> From<T> for HashedDescriptorIdentifier
where
    T: Into<u64>,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

// Specifier

#[derive(Clone, Debug)]
pub struct HashedDescriptorSpecifier(HashedDescriptorIdentifier, DescriptorSpecifier);

impl HashedDescriptorSpecifier {
    pub fn hashed(&self) -> &HashedDescriptorIdentifier {
        &self.0
    }

    pub fn inner(&self) -> &DescriptorSpecifier {
        &self.1
    }
}

impl From<DescriptorSpecifier> for HashedDescriptorSpecifier {
    fn from(value: DescriptorSpecifier) -> Self {
        let bytes = value.identifier().value().as_bytes();
        let identifier = v3::rapidhash_v3_seeded(bytes, &SEED).into();

        Self(identifier, value)
    }
}

// -------------------------------------------------------------------------------------------------

// Hashed Tag

// Tag

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HashedTag(u64, Tag);

impl HashedTag {
    pub fn hashed(&self) -> u64 {
        self.0
    }

    pub fn inner(&self) -> &Tag {
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
