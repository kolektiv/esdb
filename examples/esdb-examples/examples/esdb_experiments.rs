use std::error::Error;

use esdb::EventStream;

pub fn main() -> Result<(), Box<dyn Error>> {
    let es = EventStream::new("./data/esdb/experiments")?;

    println!("es: {es:#?}");
    println!("es is empty: {}", es.is_empty()?);
    println!("es len: {}", es.len()?);

    Ok(())
}
