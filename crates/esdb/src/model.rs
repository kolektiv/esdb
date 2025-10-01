use std::{
    error::Error,
    ops::Range,
};

use derive_more::Debug;
use fjall::Batch;

use crate::persistence::{
    Store,
    data::Data,
    index::Index,
};

// =================================================================================================
// Model
// =================================================================================================

// Stream

#[derive(Debug)]
pub struct Stream {
    data: Data,
    index: Index,
    position: Position,
}

impl Stream {
    pub fn new(store: &Store) -> Result<Self, Box<dyn Error>> {
        let data = Data::new(store.as_ref())?;
        let index = Index::new(store.as_ref())?;

        let len = data.len()?;
        let position = len.into();

        Ok(Self {
            data,
            index,
            position,
        })
    }
}

impl Stream {
    pub fn append(
        &mut self,
        batch: &mut Batch,
        events: impl IntoIterator<Item = (Vec<u8>, Descriptor, Vec<Tag>)>,
    ) -> Result<(), Box<dyn Error>> {
        for event in events {
            let pos = self.position;

            let descriptor = event.1.into();
            let tags = event.2.into_iter().map(Into::into).collect::<Vec<_>>();

            self.data.insert(batch, &event.0[..], pos);
            self.index.insert(batch, &descriptor, &tags, pos);
            self.position.increment();
        }

        Ok(())
    }
}

impl Stream {
    pub fn is_empty(&self) -> Result<bool, Box<dyn Error>> {
        self.data.is_empty()
    }

    pub fn len(&self) -> Result<u64, Box<dyn Error>> {
        self.data.len()
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
