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

// =================================================================================================
// Stream
// =================================================================================================

#[derive(Debug)]
pub struct Stream {
    data: Data,
    partitions: Partitions,
    state: State,
}

impl Stream {
    pub fn new<P>(path: P) -> Result<Self, Box<dyn Error>>
    where
        P: AsRef<Path>,
    {
        let data = Data::new(path)?;
        let partitions = Partitions::new(&data)?;
        let state = State::new(&partitions)?;

        let event_stream = Stream {
            data,
            partitions,
            state,
        };

        Ok(event_stream)
    }
}

impl Stream {
    pub fn append(&mut self, event: &[u8], type_tag: [u8; 8]) -> Result<(), Box<dyn Error>> {
        let index = self.state.index.to_be_bytes();

        let mut batch = self.data.keyspace.batch();

        let event_key = index;
        let event_value = event;
        let event_partition = &self.partitions.events;
        batch.insert(event_partition, event_key, event_value);

        let type_index_key = [&type_tag[..], &index[..]].concat();
        let type_index_value = [];
        let type_index_partition = &self.partitions.type_indices;
        batch.insert(type_index_partition, type_index_key, type_index_value);

        batch.commit()?;

        self.state.index += 1;

        Ok(())
    }
}

impl Stream {
    pub fn is_empty(&self) -> Result<bool, Box<dyn Error>> {
        is_empty(&self.partitions)
    }

    pub fn len(&self) -> Result<u64, Box<dyn Error>> {
        len(&self.partitions)
    }
}

// -------------------------------------------------------------------------------------------------

// Data

#[derive(Debug)]
struct Data {
    #[debug("Keyspace")]
    keyspace: Keyspace,
}

impl Data {
    fn new<P>(path: P) -> Result<Self, Box<dyn Error>>
    where
        P: AsRef<Path>,
    {
        let keyspace = Config::new(path).open()?;

        let data = Self { keyspace };

        Ok(data)
    }
}

// -------------------------------------------------------------------------------------------------

// Partitions

const EVENTS_PARTITION: &str = "events";
const TYPE_INDICES_PARTITION: &str = "type_indices";

#[derive(Debug)]
struct Partitions {
    #[debug("PartitionHandle(\"{}\")", events.name)]
    events: PartitionHandle,
    #[debug("PartitionHandle(\"{}\")", type_indices.name)]
    type_indices: PartitionHandle,
}

impl Partitions {
    fn new(data: &Data) -> Result<Self, Box<dyn Error>> {
        let events_options = PartitionCreateOptions::default();
        let events = data
            .keyspace
            .open_partition(EVENTS_PARTITION, events_options)?;

        let type_indices_options = PartitionCreateOptions::default();
        let type_indices = data
            .keyspace
            .open_partition(TYPE_INDICES_PARTITION, type_indices_options)?;

        let partitions = Partitions {
            events,
            type_indices,
        };

        Ok(partitions)
    }
}

// -------------------------------------------------------------------------------------------------

// State

#[derive(Debug)]
struct State {
    index: u64,
}

impl State {
    fn new(partitions: &Partitions) -> Result<Self, Box<dyn Error>> {
        let index = len(partitions)?;

        let state = Self { index };

        Ok(state)
    }
}

// =================================================================================================
// Stream Functions
// =================================================================================================

// Properties

fn is_empty(partitions: &Partitions) -> Result<bool, Box<dyn Error>> {
    len(partitions).map(|len| len == 0)
}

fn len(partitions: &Partitions) -> Result<u64, Box<dyn Error>> {
    let index_and_event = partitions.events.last_key_value()?;

    match index_and_event {
        Some((index, _)) => {
            let index = index.as_array().expect("invalid index slice");
            let index = u64::from_be_bytes(*index);

            Ok(index + 1)
        }
        None => Ok(0),
    }
}
