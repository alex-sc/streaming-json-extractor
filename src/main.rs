extern crate json_event_parser;

use json_event_parser::{JsonEvent, FromReadJsonReader};
use std::fs::File;
use std::io::{BufReader};
use std::time::{SystemTime, UNIX_EPOCH};

fn main() -> std::io::Result<()> {
    let file = File::open("./FloridaBlue_GBO_in-network-rates.json")?;
    let reader = BufReader::new(file);

    let mut parser = FromReadJsonReader::new(reader);
    let mut token_count = 0;
    let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

    // Process events one by one
    while let event = parser.read_next_event().unwrap() {
        match event {
            JsonEvent::StartObject => {},
            JsonEvent::EndObject => {},
            JsonEvent::StartArray => {},
            JsonEvent::EndArray => {},
            JsonEvent::ObjectKey(key) => {},
            JsonEvent::String(value) => {},
            JsonEvent::Number(value) => {},
            JsonEvent::Boolean(value) => {},
            JsonEvent::Null => {},
            JsonEvent::Eof => {},
        }

        token_count += 1;
        if token_count % 1_000_000 == 0 {
            let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            println!("{} in {}", token_count / 1_000_000, end.as_secs() - start.as_secs())
        }
    }

    let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    print!("{} seconds", end.as_secs() - start.as_secs());

    Ok(())
}
