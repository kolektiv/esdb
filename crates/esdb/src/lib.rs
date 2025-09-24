#![feature(slice_as_array)]

use std::{
    error::Error,
    path::Path,
};

use rocksdb::{
    ColumnFamilyDescriptor,
    IteratorMode,
    MultiThreaded,
    OptimisticTransactionDB,
    Options,
    WaitForCompactOptions,
};

const EVENTS_CF_NAME: &str = "events";

pub struct EventStream {
    opt_txn_db: OptimisticTransactionDB<MultiThreaded>,
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
        let events_cf = ColumnFamilyDescriptor::new(EVENTS_CF_NAME, Options::default());

        let mut options = Options::default();
        options.create_missing_column_families(true);
        options.create_if_missing(true);

        let opt_txn_db =
            OptimisticTransactionDB::<MultiThreaded>::open_cf_descriptors(&options, path, [
                events_cf,
            ])?;

        Ok(Self { opt_txn_db })
    }
}

impl AsRef<OptimisticTransactionDB<MultiThreaded>> for EventStream {
    fn as_ref(&self) -> &OptimisticTransactionDB<MultiThreaded> {
        &self.opt_txn_db
    }
}

impl AsMut<OptimisticTransactionDB<MultiThreaded>> for EventStream {
    fn as_mut(&mut self) -> &mut OptimisticTransactionDB<MultiThreaded> {
        &mut self.opt_txn_db
    }
}

impl Drop for EventStream {
    fn drop(&mut self) {
        let mut options = WaitForCompactOptions::default();
        options.set_abort_on_pause(true);
        options.set_flush(true);
        options.set_timeout(0);

        self.opt_txn_db
            .wait_for_compact(&options)
            .expect("failed to compact on drop");
    }
}

impl EventStream {
    /// Returns the is empty of this [`EventStream`].
    ///
    /// # Errors
    ///
    /// This function will return an error if .
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
    pub fn len(&self) -> u64 {
        let events_cf = self
            .opt_txn_db
            .cf_handle(EVENTS_CF_NAME)
            .expect("events column family not found");

        let mut events_iterator = self.opt_txn_db.iterator_cf(&events_cf, IteratorMode::End);

        if let Some(event) = events_iterator.next() {
            let index = event.expect("final event unreadable").0;
            let index = index.as_array().expect("final event index invalid");
            let index = u64::from_be_bytes(*index);

            index + 1
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {

    use serial_test::serial;

    use crate::EventStream;

    #[test]
    #[serial]
    fn new_event_stream_is_empty() {
        let event_stream = EventStream::new("./data/test/new_event_stream")
            .expect("event stream could not be created");

        assert!(event_stream.is_empty());
    }

    #[test]
    #[serial]
    fn new_event_stream_len_is_zero() {
        let event_stream = EventStream::new("./data/test/new_event_stream")
            .expect("event stream could not be created");

        assert_eq!(0, event_stream.len());
    }
}
