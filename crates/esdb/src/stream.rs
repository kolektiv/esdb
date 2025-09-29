mod data;
mod events;
mod indices;
mod state;

use std::{
    error::Error,
    path::Path,
};

use derive_more::Debug;
use fjall::Batch;

use crate::stream::{
    data::Data,
    events::Events,
    indices::Indices,
    state::State,
};

// =================================================================================================
// Stream
// =================================================================================================

trait Append {
    fn append(&self, batch: &mut Batch, event: &(&[u8], u64), position: u64);
}

#[derive(Debug)]
pub struct Stream {
    data: Data,
    events: Events,
    indices: Indices,
    state: State,
}

impl Stream {
    pub fn new<P>(path: P) -> Result<Self, Box<dyn Error>>
    where
        P: AsRef<Path>,
    {
        let data = Data::new(path)?;
        let events = Events::new(&data)?;
        let indices = Indices::new(&data)?;
        let state = State::new(&events)?;

        Ok(Self {
            data,
            events,
            indices,
            state,
        })
    }
}

impl Stream {
    pub fn append(&mut self, events: &[(&[u8], u64)]) -> Result<(), Box<dyn Error>> {
        let mut batch = self.data.batch();

        for event in events {
            let position = self.state.current();

            self.events.append(&mut batch, event, position);
            self.indices.append(&mut batch, event, position);
            self.state.increment();
        }

        batch.commit()?;

        Ok(())
    }
}

impl Stream {
    pub fn is_empty(&self) -> Result<bool, Box<dyn Error>> {
        self.events.is_empty()
    }

    pub fn len(&self) -> Result<u64, Box<dyn Error>> {
        self.events.len()
    }
}
