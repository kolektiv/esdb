use std::error::Error;

use rapidhash::v3::{
    self,
    RapidSecrets,
};

const SECRETS: RapidSecrets = RapidSecrets::seed(0x2811_2017);

pub fn main() -> Result<(), Box<dyn Error>> {
    let a = v3::rapidhash_v3_seeded(b"email:andrew@xyncro.com", &SECRETS);
    let b = v3::rapidhash_v3_seeded(b"email:andrew@xyncro.com", &SECRETS);
    let c = v3::rapidhash_v3_seeded(b"email:andrew@cherry.estate", &SECRETS);

    println!("hashed: {a}, {b}, {c}");

    Ok(())
}
