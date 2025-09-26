use std::error::Error;

use esdb::{
    stream::Stream,
    // tags::SecondaryTag,
    // value_tag,
};

pub fn main() -> Result<(), Box<dyn Error>> {
    let mut stream = Stream::new("./data/esdb/experiments")?;

    stream.append("hello world!".as_bytes(), 234u64.to_be_bytes())?;
    stream.append("oh, something else!".as_bytes(), 67u64.to_be_bytes())?;
    stream.append("goodbye cruel world...".as_bytes(), 234u64.to_be_bytes())?;

    Ok(())
}

// value_tag!(Email, "email");
// value_tag!(StudentId, "student_id");

// pub fn test(tags: &[&dyn SecondaryTag]) {
//     for t in tags {
//         t.to_string();
//     }
// }

// pub fn test_2() {
//     test(&[&Email::from("a"), &StudentId::from("b")]);
// }
