#![allow(clippy::multiple_crate_versions)]
#![feature(slice_as_array)]

use std::{
    error::Error,
    path::Path,
};

use derive_more::Debug;
use fjall::{
    Config,
    Keyspace,
    PartitionCreateOptions,
    PartitionHandle,
};

pub trait TypeTag {}

pub trait ValueTag {}

#[derive(Debug)]
pub struct EventStream {
    #[debug("Keyspace")]
    keyspace: Keyspace,
    partitions: Partitions,
}

impl EventStream {
    /// .
    ///
    /// # Errors
    ///
    /// This function will return an error if .
    pub fn new<P>(path: P) -> Result<Self, Box<dyn Error>>
    where
        P: AsRef<Path>,
    {
        let keyspace = Config::new(path).open()?;
        let partitions = Partitions::new(&keyspace)?;

        let event_stream = EventStream {
            keyspace,
            partitions,
        };

        Ok(event_stream)
    }
}

impl EventStream {
    pub fn append(kind: u32, tags: Vec<u64>, data: Vec<u8>, constraint: Option<()>) {}
}

impl EventStream {
    /// Returns the is empty of this [`EventStream`].
    ///
    /// # Errors
    ///
    /// This function will return an error if .
    pub fn is_empty(&self) -> Result<bool, Box<dyn Error>> {
        self.len().map(|len| len == 0)
    }

    /// Returns the length of this [`EventStream`].
    ///
    /// # Panics
    ///
    /// Panics if .
    ///
    /// # Errors
    ///
    /// This function will return an error if .
    pub fn len(&self) -> Result<u64, Box<dyn Error>> {
        if let Some((index, _)) = self.partitions.events.last_key_value()? {
            let index = index.as_array().expect("invalid index slice");
            let index = u64::from_be_bytes(*index);

            Ok(index + 1)
        } else {
            Ok(0)
        }
    }
}

const EVENTS_PARTITION: &str = "events";

#[derive(Debug)]
struct Partitions {
    #[debug("PartitionHandle(\"{}\")", events.name)]
    events: PartitionHandle,
}

impl Partitions {
    fn new(keyspace: &Keyspace) -> Result<Self, Box<dyn Error>> {
        let events_options = PartitionCreateOptions::default();
        let events = keyspace.open_partition(EVENTS_PARTITION, events_options)?;

        let partitions = Partitions { events };

        Ok(partitions)
    }
}
