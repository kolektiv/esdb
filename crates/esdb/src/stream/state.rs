use std::error::Error;

use crate::stream::events::Events;

#[derive(Debug)]
pub struct State {
    position: u64,
}

impl State {
    pub fn new(events: &Events) -> Result<Self, Box<dyn Error>> {
        let position = events.len()?;

        Ok(Self { position })
    }
}

impl State {
    pub fn current(&self) -> u64 {
        self.position
    }

    pub fn increment(&mut self) {
        self.position += 1;
    }
}
