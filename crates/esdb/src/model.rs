use std::{
    error::Error,
    ops::Range,
    path::Path,
};

use derive_more::Debug;
use fancy_constructor::new;

use crate::persistence::{
    self,
    Database,
    Partitions,
    ReadContext,
    WriteContext,
};

// =================================================================================================
// Model
// =================================================================================================

// Event

#[derive(new, Debug)]
pub struct Event {
    #[new(into)]
    pub data: Vec<u8>,
    #[new(into)]
    pub descriptor: Descriptor,
    #[new(into)]
    pub tags: Vec<Tag>,
}

// -------------------------------------------------------------------------------------------------

// Stream

#[derive(new, Debug)]
#[new(name(new_internal), vis())]
pub struct Stream {
    database: Database,
    partitions: Partitions,
    position: Position,
}

impl Stream {
    pub fn new<P>(path: P) -> Result<Self, Box<dyn Error>>
    where
        P: AsRef<Path>,
    {
        let database = persistence::database(path)?;
        let partitions = persistence::partitions(&database)?;

        let len = persistence::data::len(&ReadContext::new(&partitions))?;
        let position = len.into();

        Ok(Self::new_internal(database, partitions, position))
    }
}

impl Stream {
    pub fn append<E>(&mut self, events: E) -> Result<(), Box<dyn Error>>
    where
        E: IntoIterator<Item = Event>,
    {
        let mut batch = self.database.as_ref().batch();

        {
            let mut ctx = WriteContext::new(&mut batch, &self.partitions);

            for event in events {
                persistence::insert(&mut ctx, self.position, event);

                self.position.increment();
            }
        }

        batch.commit()?;

        Ok(())
    }
}

impl Stream {
    pub fn is_empty(&self) -> Result<bool, Box<dyn Error>> {
        persistence::data::is_empty(&ReadContext::new(&self.partitions))
    }

    pub fn len(&self) -> Result<u64, Box<dyn Error>> {
        persistence::data::len(&ReadContext::new(&self.partitions))
    }
}

// -------------------------------------------------------------------------------------------------

// Position

#[derive(new, Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[new(vis())]
pub struct Position(#[new(into)] u64);

impl Position {
    pub(crate) fn increment(&mut self) {
        self.0 += 1;
    }

    pub fn value(self) -> u64 {
        self.0
    }
}

impl<T> From<T> for Position
where
    T: Into<u64>,
{
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

// -------------------------------------------------------------------------------------------------

// Descriptor

// Descriptor

#[derive(new, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[new(vis())]
pub struct Descriptor(
    #[new(into)] DescriptorIdentifier,
    #[new(into)] DescriptorVersion,
);

impl Descriptor {
    pub fn identifier(&self) -> &DescriptorIdentifier {
        &self.0
    }

    pub fn version(&self) -> &DescriptorVersion {
        &self.1
    }

    pub fn take(self) -> (DescriptorIdentifier, DescriptorVersion) {
        (self.0, self.1)
    }
}

impl<T, U> From<(T, U)> for Descriptor
where
    T: Into<DescriptorIdentifier>,
    U: Into<DescriptorVersion>,
{
    fn from(value: (T, U)) -> Self {
        Self::new(value.0, value.1)
    }
}

// Identifier

#[derive(new, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[new(vis())]
pub struct DescriptorIdentifier(#[new(into)] String);

impl DescriptorIdentifier {
    pub fn value(&self) -> &str {
        &self.0
    }
}

impl<T> From<T> for DescriptorIdentifier
where
    T: Into<String>,
{
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

// Specifier

#[derive(new, Clone, Debug, Eq, PartialEq)]
#[new(vis())]
pub struct DescriptorSpecifier(
    #[new(into)] DescriptorIdentifier,
    #[new(into)] Option<Range<DescriptorVersion>>,
);

impl DescriptorSpecifier {
    pub fn identifier(&self) -> &DescriptorIdentifier {
        &self.0
    }

    pub fn range(&self) -> Option<&Range<DescriptorVersion>> {
        self.1.as_ref()
    }

    pub fn take(self) -> (DescriptorIdentifier, Option<Range<DescriptorVersion>>) {
        (self.0, self.1)
    }
}

impl<T, U> From<(T, U)> for DescriptorSpecifier
where
    T: Into<DescriptorIdentifier>,
    U: Into<Option<Range<DescriptorVersion>>>,
{
    fn from(value: (T, U)) -> Self {
        Self::new(value.0, value.1)
    }
}

// Version

#[derive(new, Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[new(vis())]
pub struct DescriptorVersion(#[new(into)] u8);

impl DescriptorVersion {
    pub fn value(self) -> u8 {
        self.0
    }
}

impl<T> From<T> for DescriptorVersion
where
    T: Into<u8>,
{
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

// -------------------------------------------------------------------------------------------------

// Tag

// Tag

#[derive(new, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[new(vis())]
pub struct Tag(#[new(into)] String);

impl Tag {
    pub fn value(&self) -> &str {
        &self.0
    }
}

impl<T> From<T> for Tag
where
    T: Into<String>,
{
    fn from(value: T) -> Self {
        Self::new(value)
    }
}
