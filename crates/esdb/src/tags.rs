use std::{
    borrow::Borrow,
    hash::{
        Hash,
        Hasher,
    },
};

use multimap::MultiMap;
use rapidhash::v3::{
    self,
    RapidSecrets,
};

pub static HASH_DATA: RapidSecrets = RapidSecrets::seed(0x2811_2017);

pub trait TagHash {
    fn tag_hash(&self, state: &mut dyn Hasher);
}

impl Hash for dyn SecondaryTag {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tag_hash(state);
    }
}

impl<T> TagHash for T
where
    T: Hash + ?Sized,
{
    fn tag_hash(&self, mut state: &mut dyn Hasher) {
        self.hash(&mut state);
    }
}

pub trait Tag: TagHash + ToString {}

pub trait PrimaryTag: Tag {}

pub trait SecondaryTag: Tag {}

pub trait ValueTag: SecondaryTag {
    const NAME: &str;
}

// Example Implementation (would be macro-generated)

#[macro_export]
macro_rules! value_tag {
    ($type:ident, $name:literal) => {
        pub struct $type(String);

        impl ::std::fmt::Display for $type {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                write!(f, "{}:{}", <Self as ::esdb::tags::ValueTag>::NAME, self.0)
            }
        }

        impl<T> ::std::convert::From<T> for $type
        where
            T: Into<String>,
        {
            fn from(value: T) -> Self {
                Self(value.into())
            }
        }

        impl ::std::hash::Hash for $type {
            fn hash<H: ::std::hash::Hasher>(&self, state: &mut H) {
                state.write_u64(::rapidhash::v3::rapidhash_v3_seeded(
                    self.to_string().as_bytes(),
                    &::esdb::tags::HASH_DATA,
                ));
            }
        }

        impl ::esdb::tags::Tag for $type {}

        impl ::esdb::tags::SecondaryTag for $type {}

        impl ::esdb::tags::ValueTag for $type {
            const NAME: &str = $name;
        }
    };
}

#[derive(Debug, Default)]
pub struct TagMap {
    map: MultiMap<String, String>,
}

impl TagMap {
    pub fn add<K, V>(&mut self, key: K, value: V)
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.map.insert(key.into(), value.into());
    }

    pub fn get<K>(&self, key: &K) -> Option<&Vec<String>>
    where
        K: Eq + Hash,
        String: Borrow<K>,
    {
        self.map.get_vec(key)
    }
}

impl TagMap {
    #[allow(dead_code)]
    fn hashes(&self) -> Vec<[u8; 8]> {
        self.map
            .iter_all()
            .flat_map(move |(key, values)| {
                values.iter().map(move |value| {
                    v3::rapidhash_v3_seeded(format!("{key}:{value}").as_bytes(), &HASH_DATA)
                        .to_be_bytes()
                })
            })
            .collect()
    }
}
