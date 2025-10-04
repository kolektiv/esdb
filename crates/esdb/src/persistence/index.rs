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
        WriteContext,
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

pub fn insert(ctx: &mut WriteContext<'_>, position: Position, event: &HashedEvent) {
    descriptor::insert(ctx, position, &event.descriptor);
    tags::insert(ctx, position, &event.tags);
}

// -------------------------------------------------------------------------------------------------

// Descriptor

mod descriptor {
    use crate::{
        model::Position,
        persistence::{
            HashedDescriptor,
            WriteContext,
        },
    };

    static HASH_LEN: usize = size_of::<u64>();

    //  Insertion

    pub fn insert(ctx: &mut WriteContext<'_>, position: Position, descriptor: &HashedDescriptor) {
        forward::insert(ctx, position, descriptor);
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
                HashedDescriptorIdentifier,
                HashedDescriptorSpecifier,
                POSITION_LEN,
                ReadContext,
                WriteContext,
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

        pub fn insert(
            ctx: &mut WriteContext<'_>,
            position: Position,
            descriptor: &HashedDescriptor,
        ) {
            let mut key = [0u8; KEY_LEN];

            let identifier = descriptor.identifer();

            write_key(&mut key, position, identifier);

            let value = descriptor.version().value().to_be_bytes();

            ctx.batch.insert(&ctx.partitions.index, key, value);
        }

        // Iteration

        pub fn iterate(
            ctx: &ReadContext<'_>,
            position: Option<Position>,
            specifier: &HashedDescriptorSpecifier,
        ) -> impl Iterator<Item = u64> {
            let iterator = match position {
                Some(position) => iterate_range(ctx, position, specifier),
                None => iterate_prefix(ctx, specifier),
            };

            let version_range = specifier.range();
            let version_bounds = version_range
                .as_ref()
                .map_or((u8::MIN, u8::MAX), |r| (r.start.value(), r.end.value()));

            let version_min = version_bounds.0;
            let version_max = version_bounds.1;
            let version_filter = version_min > u8::MIN || version_max < u8::MAX;

            iterator.filter_map(move |kv| {
                let (k, v) = kv.expect("invalid key/value during iteration");

                if version_filter {
                    let mut v = &v[..];

                    let version = v.get_u8();

                    if !(version_min..version_max).contains(&version) {
                        return None;
                    }
                }

                let mut k = &k[..];

                k.advance(ID_LEN + HASH_LEN);

                let position = k.get_u64();

                Some(position)
            })
        }

        fn iterate_prefix(
            ctx: &ReadContext<'_>,
            specifier: &HashedDescriptorSpecifier,
        ) -> Box<dyn Iterator<Item = Result<(Slice, Slice), Error>>> {
            let mut prefix = [0u8; PREFIX_LEN];

            let identifier = specifier.identifer();

            write_prefix(&mut prefix, identifier);

            Box::new(ctx.partitions.index.prefix(prefix))
        }

        fn iterate_range(
            ctx: &ReadContext<'_>,
            position: Position,
            specifier: &HashedDescriptorSpecifier,
        ) -> Box<dyn Iterator<Item = Result<(Slice, Slice), Error>>> {
            let mut lower = [0u8; KEY_LEN];
            let mut upper = [0u8; KEY_LEN];

            let identifier = specifier.identifer();

            write_key(&mut lower, position, identifier);

            let position = Position::from(u64::MAX);

            write_key(&mut upper, position, identifier);

            Box::new(ctx.partitions.index.range(lower..=upper))
        }

        // Keys/Prefixes

        fn write_key(
            key: &mut [u8; KEY_LEN],
            position: Position,
            identifier: &HashedDescriptorIdentifier,
        ) {
            let mut key = &mut key[..];

            let index_id = INDEX_ID;
            let identifier = identifier.hash();
            let position = position.value();

            key.put_u8(index_id);
            key.put_u64(identifier);
            key.put_u64(position);
        }

        fn write_prefix(prefix: &mut [u8; PREFIX_LEN], identifier: &HashedDescriptorIdentifier) {
            let mut prefix = &mut prefix[..];

            let index_id = INDEX_ID;
            let identifier = identifier.hash();

            prefix.put_u8(index_id);
            prefix.put_u64(identifier);
        }
    }
}

// #[must_use]
// pub fn view(&self, descriptor: HashedDescriptorSpecifier) ->
// DescriptorForwardIndexView {     DescriptorForwardIndexView::new(self.index.
// clone(), descriptor) }

// // View
// #[derive(Debug)]
// pub struct DescriptorForwardIndexView {
//     #[debug("PartitionHandle(\"{}\")", index.name)]
//     index: PartitionHandle,
//     specifier: HashedDescriptorSpecifier,
// }

// impl DescriptorForwardIndexView {
//     fn new(index: PartitionHandle, specifier: HashedDescriptorSpecifier) ->
// Self {         Self { index, specifier }
//     }
// }

// impl IntoIterator for DescriptorForwardIndexView {
//     type IntoIter = DescriptorForwardIndexIterator;
//     type Item = u64;

//     fn into_iter(self) -> Self::IntoIter {
//         let mut prefix = [0u8; DESCRIPTOR_FORWARD_INDEX_PREFIX_LEN];

//         get_descriptor_forward_prefix(&mut prefix, &self.specifier);

//         let version_range = self.specifier.as_ref().range();
//         let version_bounds = version_range
//             .as_ref()
//             .map_or((u8::MIN, u8::MAX), |r| (r.start.value(),
// r.end.value()));

//         let version_min = version_bounds.0;
//         let version_max = version_bounds.1;
//         let version_filter = version_min > u8::MIN || version_max < u8::MAX;

//         let iterator = Box::new(self.index.prefix(prefix).filter_map(move
// |kv| {             let (k, v) = kv.expect("invalid key/value during
// iteration");

//             if version_filter {
//                 let version = v.as_ref().get_u8();

//                 if !(version_min..version_max).contains(&version) {
//                     return None;
//                 }
//             }

//             let mut k = &k[..];

//             k.advance(ID_LEN + DESCRIPTOR_HASH_LEN);

//             let position = k.get_u64();

//             Some(position)
//         }));

//         DescriptorForwardIndexIterator::new(iterator)
//     }
// }

// // Iterator

// #[derive(Debug)]
// pub struct DescriptorForwardIndexIterator {
//     #[debug(skip)]
//     iterator: Box<dyn Iterator<Item = u64>>,
// }

// impl DescriptorForwardIndexIterator {
//     fn new(iterator: Box<dyn Iterator<Item = u64>>) -> Self {
//         Self { iterator }
//     }
// }

// impl Iterator for DescriptorForwardIndexIterator {
//     type Item = u64;

//     fn next(&mut self) -> Option<Self::Item> {
//         self.iterator.next()
//     }
// }

// -------------------------------------------------------------------------------------------------

// Tag

mod tags {
    use crate::{
        model::Position,
        persistence::{
            HashedTag,
            WriteContext,
        },
    };

    static HASH_LEN: usize = size_of::<u64>();

    // Insertion

    pub fn insert(ctx: &mut WriteContext<'_>, position: Position, tags: &[HashedTag]) {
        forward::insert(ctx, position, tags);
    }

    // Forward Index

    mod forward {
        use bytes::BufMut as _;

        use crate::{
            model::Position,
            persistence::{
                HashedTag,
                POSITION_LEN,
                WriteContext,
                index::{
                    ID_LEN,
                    tags::HASH_LEN,
                },
            },
        };

        static INDEX_ID: u8 = 1;
        static KEY_LEN: usize = ID_LEN + HASH_LEN + POSITION_LEN;

        // Insertion

        pub fn insert(ctx: &mut WriteContext<'_>, position: Position, tags: &[HashedTag]) {
            let mut key = [0u8; KEY_LEN];

            for tag in tags {
                write_key(&mut key, position, tag);

                ctx.batch.insert(&ctx.partitions.index, key, []);
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
