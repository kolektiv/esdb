use std::error::Error;

use fjall::{
    PartitionCreateOptions,
    PartitionHandle,
};

use crate::{
    model::Position,
    persistence::{
        Database,
        HashedEvent,
        Write,
    },
};

// =================================================================================================
// Index
// =================================================================================================

static ID_LEN: usize = size_of::<u8>();
static PARTITION_NAME: &str = "index";

// Partitions

pub fn partition(database: &Database) -> Result<PartitionHandle, Box<dyn Error>> {
    Ok(database
        .as_ref()
        .open_partition(PARTITION_NAME, PartitionCreateOptions::default())?)
}

// Partition Insertion

pub fn insert(write: &mut Write<'_>, position: Position, event: &HashedEvent) {
    descriptor::insert(write, position, &event.descriptor);
    tags::insert(write, position, &event.tags);
}

// -------------------------------------------------------------------------------------------------

// Descriptor

mod descriptor {
    use crate::{
        model::Position,
        persistence::{
            HashedDescriptor,
            Write,
        },
    };

    static HASH_LEN: usize = size_of::<u64>();

    //  Insertion

    pub fn insert(write: &mut Write<'_>, position: Position, descriptor: &HashedDescriptor) {
        forward::insert(write, position, descriptor);
    }

    // Forward Index

    mod forward {
        use bytes::{
            Buf as _,
            BufMut as _,
        };
        use fjall::{
            Error,
            Slice,
        };

        use crate::{
            model::Position,
            persistence::{
                HashedDescriptor,
                HashedIdentifier,
                HashedSpecifier,
                POSITION_LEN,
                Read,
                Write,
                index::{
                    ID_LEN,
                    descriptor::HASH_LEN,
                },
            },
        };

        static INDEX_ID: u8 = 0;
        static KEY_LEN: usize = ID_LEN + HASH_LEN + POSITION_LEN;
        static PREFIX_LEN: usize = ID_LEN + HASH_LEN;

        //  Insertion

        pub fn insert(write: &mut Write<'_>, position: Position, descriptor: &HashedDescriptor) {
            let mut key = [0u8; KEY_LEN];

            let identifier = descriptor.identifer();

            write_key(&mut key, position, identifier);

            let value = descriptor.version().value().to_be_bytes();

            write.batch.insert(&write.partitions.index, key, value);
        }

        // Iteration

        pub fn iterate(
            read: &Read<'_>,
            position: Option<Position>,
            specifier: &HashedSpecifier,
        ) -> impl Iterator<Item = u64> {
            let iterator = match position {
                Some(position) => iterate_range(read, position, specifier),
                None => iterate_prefix(read, specifier),
            };

            let version_range = specifier.range();
            let version_bounds = version_range
                .as_ref()
                .map_or((u8::MIN, u8::MAX), |r| (r.start.value(), r.end.value()));

            let version_min = version_bounds.0;
            let version_max = version_bounds.1;
            let version_filter = version_min > u8::MIN || version_max < u8::MAX;

            iterator.filter_map(move |key_value| {
                let (key, value) = key_value.expect("invalid key/value during iteration");

                if version_filter {
                    let mut value = &value[..];

                    let version = value.get_u8();

                    if !(version_min..version_max).contains(&version) {
                        return None;
                    }
                }

                let mut key = &key[..];

                key.advance(ID_LEN + HASH_LEN);

                let position = key.get_u64();

                Some(position)
            })
        }

        fn iterate_prefix(
            read: &Read<'_>,
            specifier: &HashedSpecifier,
        ) -> Box<dyn Iterator<Item = Result<(Slice, Slice), Error>>> {
            let mut prefix = [0u8; PREFIX_LEN];

            let identifier = specifier.identifer();

            write_prefix(&mut prefix, identifier);

            Box::new(read.partitions.index.prefix(prefix))
        }

        fn iterate_range(
            read: &Read<'_>,
            position: Position,
            specifier: &HashedSpecifier,
        ) -> Box<dyn Iterator<Item = Result<(Slice, Slice), Error>>> {
            let mut lower = [0u8; KEY_LEN];
            let mut upper = [0u8; KEY_LEN];

            let identifier = specifier.identifer();

            write_key(&mut lower, position, identifier);

            let position = Position::from(u64::MAX);

            write_key(&mut upper, position, identifier);

            Box::new(read.partitions.index.range(lower..=upper))
        }

        // Keys/Prefixes

        fn write_key(key: &mut [u8; KEY_LEN], position: Position, identifier: &HashedIdentifier) {
            let mut key = &mut key[..];

            let index_id = INDEX_ID;
            let identifier = identifier.hash();
            let position = position.value();

            key.put_u8(index_id);
            key.put_u64(identifier);
            key.put_u64(position);
        }

        fn write_prefix(prefix: &mut [u8; PREFIX_LEN], identifier: &HashedIdentifier) {
            let mut prefix = &mut prefix[..];

            let index_id = INDEX_ID;
            let identifier = identifier.hash();

            prefix.put_u8(index_id);
            prefix.put_u64(identifier);
        }
    }
}

// -------------------------------------------------------------------------------------------------

// Tags

mod tags {
    use crate::{
        model::Position,
        persistence::{
            HashedTag,
            Write,
        },
    };

    static HASH_LEN: usize = size_of::<u64>();

    // Insertion

    pub fn insert(write: &mut Write<'_>, position: Position, tags: &[HashedTag]) {
        forward::insert(write, position, tags);
    }

    // Forward Index

    mod forward {
        use bytes::BufMut as _;

        use crate::{
            model::Position,
            persistence::{
                HashedTag,
                POSITION_LEN,
                Write,
                index::{
                    ID_LEN,
                    tags::HASH_LEN,
                },
            },
        };

        static INDEX_ID: u8 = 1;
        static KEY_LEN: usize = ID_LEN + HASH_LEN + POSITION_LEN;

        // Insertion

        pub fn insert(write: &mut Write<'_>, position: Position, tags: &[HashedTag]) {
            let mut key = [0u8; KEY_LEN];

            for tag in tags {
                write_key(&mut key, position, tag);

                write.batch.insert(&write.partitions.index, key, []);
            }
        }

        // Keys/Prefixes

        fn write_key(key: &mut [u8; KEY_LEN], position: Position, tag: &HashedTag) {
            let mut key = &mut key[..];

            let index_id = INDEX_ID;
            let tag = tag.hash();
            let position = position.value();

            key.put_u8(index_id);
            key.put_u64(tag);
            key.put_u64(position);
        }
    }
}
