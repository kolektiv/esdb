mod events;
mod indices;
mod state;

use std::error::Error;

use derive_more::Debug;
use fjall::Batch;

use crate::{
    data::Data,
    stream::{
        events::{
            Events,
            Write as _,
        },
        indices::{
            HashedTypeSpecifier,
            Indices,
        },
        state::State,
    },
};

// =================================================================================================
// Stream
// =================================================================================================

// TODO: State moves out of stream, and up one level when we combine stream and
// meta

#[derive(Debug)]
pub struct Stream {
    events: Events,
    indices: Indices,
    state: State,
}

impl Stream {
    pub fn new(data: &Data) -> Result<Self, Box<dyn Error>> {
        let events = Events::new(data)?;
        let indices = Indices::new(data)?;
        let state = State::new(&events)?;

        Ok(Self {
            events,
            indices,
            state,
        })
    }
}

impl Stream {
    pub fn append(
        &mut self,
        batch: &mut Batch,
        events: &[(&[u8], &[u64], HashedTypeSpecifier)],
    ) -> Result<(), Box<dyn Error>> {
        for event in events {
            let position = self.state.current();

            self.events.write(batch, event.0, position);
            self.indices.insert(batch, &event.2, position);
            self.state.increment();
        }

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
