use std::error::Error;

use crate::stream::events::Events;

#[derive(Debug)]
pub struct State {
    index: u64,
}

impl State {
    pub fn new(events: &Events) -> Result<Self, Box<dyn Error>> {
        let index = events.len()?;

        Ok(Self { index })
    }
}

impl State {
    pub fn bytes(&self) -> [u8; 8] {
        self.index.to_be_bytes()
    }

    pub fn increment(&mut self) {
        self.index += 1;
    }
}
