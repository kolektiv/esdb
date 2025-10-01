pub mod data;
pub mod index;

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

use crate::model::{
    Descriptor,
    DescriptorSpecifier,
    Tag,
};

// =================================================================================================
// Persistence
// =================================================================================================

// Configuration

static SEED: RapidSecrets = RapidSecrets::seed(0x2811_2017);

// -------------------------------------------------------------------------------------------------

// Store

#[derive(Debug)]
pub struct Store {
    #[debug("Keyspace")]
    keyspace: Keyspace,
}

impl Store {
    pub fn new<P>(path: P) -> Result<Self, Box<dyn Error>>
    where
        P: AsRef<Path>,
    {
        let keyspace = Config::new(path).open()?;

        Ok(Self { keyspace })
    }
}

impl Store {
    #[must_use]
    pub fn batch(&self) -> Batch {
        self.keyspace.batch()
    }
}

impl AsRef<Keyspace> for Store {
    fn as_ref(&self) -> &Keyspace {
        &self.keyspace
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
        let bytes = value.identifier().value().as_bytes();
        let identifier = v3::rapidhash_v3_seeded(bytes, &SEED).into();

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
