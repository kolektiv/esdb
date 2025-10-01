use std::{
    error::Error,
    ops::Range,
    path::Path,
};

use derive_more::Debug;

use crate::persistence::{
    Database,
    Store,
    data::Data,
};

// =================================================================================================
// Model
// =================================================================================================

// Event

#[derive(Debug)]
pub struct Event {
    pub data: Vec<u8>,
    pub descriptor: Descriptor,
    pub tags: Vec<Tag>,
}

impl Event {
    pub fn new(
        data: impl Into<Vec<u8>>,
        descriptor: impl Into<Descriptor>,
        tags: impl Into<Vec<Tag>>,
    ) -> Self {
        let data = data.into();
        let descriptor = descriptor.into();
        let tags = tags.into();

        Self {
            data,
            descriptor,
            tags,
        }
    }
}

// -------------------------------------------------------------------------------------------------

// Stream

#[derive(Debug)]
pub struct Stream {
    database: Database,
    position: Position,
    store: Store,
}

impl Stream {
    pub fn new<P>(path: P) -> Result<Self, Box<dyn Error>>
    where
        P: AsRef<Path>,
    {
        let database = Database::new(path)?;
        let store = Store::new(&database)?;
        let position = AsRef::<Data>::as_ref(&store).len().map(Into::into)?;

        Ok(Self {
            database,
            position,
            store,
        })
    }
}

impl Stream {
    pub fn append<E>(&mut self, events: E) -> Result<(), Box<dyn Error>>
    where
        E: IntoIterator<Item = Event>,
    {
        let mut batch = self.database.batch();

        for event in events {
            self.store.insert(&mut batch, self.position, event);
            self.position.increment();
        }

        batch.commit()?;

        Ok(())
    }
}

impl Stream {
    pub fn is_empty(&self) -> Result<bool, Box<dyn Error>> {
        AsRef::<Data>::as_ref(&self.store).is_empty()
    }

    pub fn len(&self) -> Result<u64, Box<dyn Error>> {
        AsRef::<Data>::as_ref(&self.store).len()
    }
}

// -------------------------------------------------------------------------------------------------

// Position

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Position(u64);

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
        Self(value.into())
    }
}

// -------------------------------------------------------------------------------------------------

// Descriptor

// Descriptor

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Descriptor(DescriptorIdentifier, DescriptorVersion);

impl Descriptor {
    pub fn identifier(&self) -> &DescriptorIdentifier {
        &self.0
    }

    pub fn version(&self) -> &DescriptorVersion {
        &self.1
    }
}

impl<T, U> From<(T, U)> for Descriptor
where
    T: Into<DescriptorIdentifier>,
    U: Into<DescriptorVersion>,
{
    fn from(value: (T, U)) -> Self {
        Self(value.0.into(), value.1.into())
    }
}

// Identifier

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DescriptorIdentifier(String);

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
        Self(value.into())
    }
}

// Specifier

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescriptorSpecifier(DescriptorIdentifier, Option<Range<DescriptorVersion>>);

impl DescriptorSpecifier {
    pub fn identifier(&self) -> &DescriptorIdentifier {
        &self.0
    }

    pub fn range(&self) -> &Option<Range<DescriptorVersion>> {
        &self.1
    }
}

impl<T, U> From<(T, U)> for DescriptorSpecifier
where
    T: Into<DescriptorIdentifier>,
    U: Into<Option<Range<DescriptorVersion>>>,
{
    fn from(value: (T, U)) -> Self {
        Self(value.0.into(), value.1.into())
    }
}

// Version

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DescriptorVersion(u8);

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
        Self(value.into())
    }
}

// -------------------------------------------------------------------------------------------------

// Tag

// Tag

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Tag(String);

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
        Self(value.into())
    }
}
