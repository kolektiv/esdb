use std::error::Error;

use esdb::{
    Event,
    Stream,
    persistence,
};
use fjall::PartitionCreateOptions;

static PATH: &str = "./data/esdb/experiments";

pub fn main() -> Result<(), Box<dyn Error>> {
    {
        let mut stream = Stream::new(PATH)?;

        stream.append(vec![
            Event::new("hello world!", ("StudentSubscribedToCourse", 0), vec![
                "student:3242".into(),
                "course:523".into(),
            ]),
            Event::new("oh, no!", ("CourseCapacityChanged", 0), vec![
                "course:523".into(),
            ]),
            Event::new("goodbye world...", ("StudentSubscribedToCourse", 1), vec![
                "student:7642".into(),
                "course:63".into(),
            ]),
        ])?;
    }

    let database = persistence::database(PATH)?;

    for partition in database.as_ref().list_partitions() {
        let partition_options = PartitionCreateOptions::default();
        let partition = database
            .as_ref()
            .open_partition(&partition, partition_options)?;

        println!("Partition: {}", partition.name);

        for kv in partition.iter() {
            let (k, v) = kv?;

            println!("{k:?}: {v:?}");
        }
    }

    Ok(())
}
