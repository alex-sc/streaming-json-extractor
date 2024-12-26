extern crate json_event_parser;
extern crate log;

use std::alloc::System;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use json_event_parser::{FromReadJsonReader, JsonEvent};

#[derive(Debug, Eq, PartialEq, Clone)]
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
    token: JsonEventOwned,
    cnt: i64,
    matches: i64,
    start: Duration
}

impl State {

    pub fn new(parser: FromReadJsonReader<BufReader<File>>) -> State {
        State {
            parser,
            start: SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            token: JsonEventOwned::Null, cnt: 0, matches: 0
        }
    }

    pub(crate) fn next_token(&mut self) {
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
        self.token = owned_event;
        self.cnt += 1;
        if self.cnt % 1_000_000 == 0 {
            let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            println!("{} M in {} seconds, matches: {}", self.cnt / 1_000_000, end.as_secs() - self.start.as_secs(), self.matches)
        }
        if self.cnt > 1000 {
            //panic!("")
        }
    }
}

fn parse(state: &mut State, store: bool, array_level: i32) -> JsonValue {
    let token = state.token.clone();
    state.next_token();
    match token {
        JsonEventOwned::StartObject => {
            let mut map = HashMap::new();
            let mut is_match = false;
            while state.token != JsonEventOwned::EndObject {
                match state.token.clone() {
                    JsonEventOwned::ObjectKey(key) => {
                        let field_name_value = key;
                        let store_new = store || "in_network" == field_name_value;
                        state.next_token();
                        let field_value = parse(state, store_new, array_level);
                        if "billing_code_type" == field_name_value {
                            is_match |= JsonValue::String("CPT".parse().unwrap()) == field_value;
                        }
                        if store {
                            map.insert(field_name_value, field_value);
                        }
                    }
                    _ => panic!("Invalid state"),
                }
            }

            if is_match {
                // println!("{:?}", map);
                state.matches += 1;
            }

            state.next_token();
            JsonValue::Object(map)
        }
        JsonEventOwned::StartArray => {
            let mut arr = Vec::new();
            while state.token != JsonEventOwned::EndArray {
                let obj = parse(state, store, array_level + 1);
                if store && array_level > 0 {
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
    state.next_token();
    parse(&mut state, false, 0);

    let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    println!("{} seconds", end.as_secs() - start.as_secs());

    Ok(())
}
