use std::error::Error;

use fjall::{
    PartitionCreateOptions,
    PartitionHandle,
};

use crate::persistence::{
    Database,
    HashedEvent,
    Write,
};

// =================================================================================================
// Reference
// =================================================================================================

static ID_LEN: usize = size_of::<u8>();
static PARTITION_NAME: &str = "reference";

// Partition

pub fn partition(database: &Database) -> Result<PartitionHandle, Box<dyn Error>> {
    Ok(database
        .as_ref()
        .open_partition(PARTITION_NAME, PartitionCreateOptions::default())?)
}

// Insertion

pub fn insert(write: &mut Write<'_>, event: &HashedEvent) {
    descriptor::insert(write, &event.descriptor);
    tags::insert(write, &event.tags);
}

// -------------------------------------------------------------------------------------------------

// Descriptor

mod descriptor {
    use crate::persistence::{
        HashedDescriptor,
        Write,
    };

    static HASH_LEN: usize = size_of::<u64>();

    // Insertion

    pub fn insert(write: &mut Write<'_>, descriptor: &HashedDescriptor) {
        lookup::insert(write, descriptor);
    }

    // Lookup Reference

    mod lookup {
        use bytes::BufMut as _;

        use crate::persistence::{
            HashedDescriptor,
            Write,
            reference::{
                ID_LEN,
                descriptor::HASH_LEN,
            },
        };

        static REFERENCE_ID: u8 = 0;
        static KEY_LEN: usize = ID_LEN + HASH_LEN;

        // Insertion

        pub fn insert(write: &mut Write<'_>, descriptor: &HashedDescriptor) {
            let mut key = [0u8; KEY_LEN];

            write_key(&mut key, descriptor);

            let value = descriptor.identifer().value().as_bytes();

            write.batch.insert(&write.partitions.reference, key, value);
        }

        // Keys/Prefixes

        fn write_key(key: &mut [u8; KEY_LEN], descriptor: &HashedDescriptor) {
            let mut key = &mut key[..];

            let reference_id = REFERENCE_ID;
            let descriptor_identifier = descriptor.identifer().hash();

            key.put_u8(reference_id);
            key.put_u64(descriptor_identifier);
        }
    }
}

// -------------------------------------------------------------------------------------------------

// Tags

mod tags {
    use crate::persistence::{
        HashedTag,
        Write,
    };

    static HASH_LEN: usize = size_of::<u64>();

    // Insertion

    pub fn insert(write: &mut Write<'_>, tags: &[HashedTag]) {
        lookup::insert(write, tags);
    }

    // Lookup Reference

    mod lookup {
        use bytes::BufMut as _;

        use crate::persistence::{
            HashedTag,
            Write,
            reference::{
                ID_LEN,
                tags::HASH_LEN,
            },
        };

        static REFERENCE_ID: u8 = 1;
        static KEY_LEN: usize = ID_LEN + HASH_LEN;

        // Insertion

        pub fn insert(write: &mut Write<'_>, tags: &[HashedTag]) {
            let mut key = [0u8; KEY_LEN];

            for tag in tags {
                write_key(&mut key, tag);

                let value = tag.value().as_bytes();

                write.batch.insert(&write.partitions.reference, key, value);
            }
        }

        // Keys/Prefixes

        fn write_key(key: &mut [u8; KEY_LEN], tag: &HashedTag) {
            let mut key = &mut key[..];

            let reference_id = REFERENCE_ID;
            let tag = tag.hash();

            key.put_u8(reference_id);
            key.put_u64(tag);
        }
    }
}
