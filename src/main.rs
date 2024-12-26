extern crate json_event_parser;
extern crate log;

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use json_event_parser::{FromReadJsonReader, JsonEvent};

#[derive(Debug, Eq, PartialEq)]
pub enum JsonValue {
    Object(HashMap<String, JsonValue>), // A JSON object
    Array(Vec<JsonValue>),             // A JSON array
    String(String),                    // A JSON string
    Number(String),                    // A JSON number
    Boolean(bool),                   // A JSON boolean
    Null,                              // A JSON null
}

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub enum JsonEventOwned {
    String(String),
    Number(String),
    Boolean(bool),
    Null,
    StartArray,
    EndArray,
    StartObject,
    EndObject,
    ObjectKey(String),
    Eof,
}

pub struct State {
    parser: FromReadJsonReader<BufReader<File>>,
    token: Option<JsonEventOwned>,
    cnt: i64,
    matches: i64,
    start: Duration
}

impl State {

    pub fn new(parser: FromReadJsonReader<BufReader<File>>) -> State {
        State {
            parser,
            start: SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            token: None, cnt: 0, matches: 0
        }
    }

    pub(crate) fn next_token(&mut self) -> JsonEventOwned {
        // Transform the borrowed `JsonEvent` into an owned version with a `'static` lifetime.
        let next = self.parser.read_next_event().unwrap();
        let owned_event = match next {
            JsonEvent::String(value) => JsonEventOwned::String(value.to_string()),
            JsonEvent::Number(value) => JsonEventOwned::Number(value.to_string()),
            JsonEvent::Boolean(value) => JsonEventOwned::Boolean(value.to_owned()),
            JsonEvent::Null => JsonEventOwned::Null,
            JsonEvent::StartObject => JsonEventOwned::StartObject,
            JsonEvent::EndObject => JsonEventOwned::EndObject,
            JsonEvent::StartArray => JsonEventOwned::StartArray,
            JsonEvent::EndArray => JsonEventOwned::EndArray,
            JsonEvent::ObjectKey(value) => JsonEventOwned::ObjectKey(value.to_string()),
            JsonEvent::Eof => JsonEventOwned::Eof,
        };
        self.token = Some(owned_event.clone());
        self.cnt += 1;
        if self.cnt % 1_000_000 == 0 {
            let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            println!("{} M in {} seconds, matches: {}", self.cnt / 1_000_000, end.as_secs() - self.start.as_secs(), self.matches)
        }
        owned_event
    }

    pub(crate) fn curr_token(&self) -> JsonEventOwned {
        // Safely clone the current token, assuming it has been initialized.
        self.token.clone().expect("No current token available")
    }
}

fn parse(state: &mut State, parent: Option<&str>, store: bool) -> JsonValue {
    let token = match parent {
        None => state.next_token(),
        Some(_) => state.curr_token(),
    };
    state.next_token();
    match token {
        JsonEventOwned::StartObject => {
            let mut store_new = store;
            if parent.is_some() && !store_new {
                store_new = "in_network" == parent.unwrap();
            }
            let mut map = HashMap::new();
            while state.curr_token() != JsonEventOwned::EndObject {
                let field_name = state.curr_token();
                match field_name {
                    JsonEventOwned::ObjectKey(key) => {
                        let field_name_value = key.to_string().clone();
                        state.next_token();
                        map.insert(field_name_value, parse(state, Some(&key), store_new));
                    }
                    _ => panic!("Invalid state"),
                }
            }

            let value = &map.get("billing_code_type");
            if value.is_some() && JsonValue::String("CPT".parse().unwrap()).eq(value.unwrap()) {
                // println!("{:?}", map);
                state.matches += 1;
            }

            state.next_token();
            JsonValue::Object(map)
        }
        JsonEventOwned::StartArray => {
            let mut arr = Vec::new();
            while state.curr_token() != JsonEventOwned::EndArray {
                let obj = parse(state, parent, store);
                if store {
                    arr.push(obj);
                }
            }
            state.next_token();
            JsonValue::Array(arr)
        }
        JsonEventOwned::String(value) => JsonValue::String(value),
        JsonEventOwned::Number(value) => JsonValue::Number(value),
        JsonEventOwned::Boolean(value) => JsonValue::Boolean(value),
        JsonEventOwned::Null => JsonValue::Null,
        _ => panic!("Invalid state"),
    }
}

fn main() -> std::io::Result<()> {
    let file = File::open("./FloridaBlue_GBO_in-network-rates.json")?;
    let reader = BufReader::new(file);

    let parser = FromReadJsonReader::new(reader);
    let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

    let mut state = State::new(parser);
    parse(&mut state, None, false);

    let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    println!("{} seconds", end.as_secs() - start.as_secs());

    Ok(())
}
