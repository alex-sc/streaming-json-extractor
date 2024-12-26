extern crate json_event_parser;
extern crate log;
extern crate qjsonrs;

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use qjsonrs::{JsonStream, JsonToken, JsonTokenIterator};

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
pub enum JsonTokenOwned {
    /// The start of an object, a.k.a. '{'
    StartObject,
    /// The end of an object, a.k.a. '}'
    EndObject,
    /// The start of an array, a.k.a. '['
    StartArray,
    /// The end of an object, a.k.a. ']'
    EndArray,
    /// The token 'null'
    JsNull,
    /// Either 'true' or 'false'
    JsBoolean(bool),
    /// A number, unparsed. i.e. '-123.456e-789'
    JsNumber(String),
    /// A JSON string in a value context.
    JsString(String),
    /// A JSON string in the context of a key in a JSON object.
    JsKey(String),
    JsEof,
}

pub struct State {
    parser: JsonStream<BufReader<File>>,
    token: JsonTokenOwned,
    cnt: i64,
    matches: i64,
    start: Duration
}

impl State {

    pub fn new(parser: JsonStream<BufReader<File>>) -> State {
        State {
            parser,
            start: SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            token: JsonTokenOwned::JsNull, cnt: 0, matches: 0
        }
    }

    pub(crate) fn next_token(&mut self) {
        // Transform the borrowed `JsonEvent` into an owned version with a `'static` lifetime.
        let next = self.parser.next().unwrap();
        if next.is_none() {
            self.token = JsonTokenOwned::JsEof;
            return;
        }
        let owned_event = match next.unwrap() {
            JsonToken::JsString(value) => JsonTokenOwned::JsString(value.into()),
            JsonToken::JsNumber(value) => JsonTokenOwned::JsNumber(value.to_string()),
            JsonToken::JsBoolean(value) => JsonTokenOwned::JsBoolean(value.to_owned()),
            JsonToken::JsNull => JsonTokenOwned::JsNull,
            JsonToken::StartObject => JsonTokenOwned::StartObject,
            JsonToken::EndObject => JsonTokenOwned::EndObject,
            JsonToken::StartArray => JsonTokenOwned::StartArray,
            JsonToken::EndArray => JsonTokenOwned::EndArray,
            JsonToken::JsKey(value) => JsonTokenOwned::JsKey(value.into())
        };
        self.token = owned_event;
        self.cnt += 1;
        if self.cnt % 1_000_000 == 0 {
            let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            println!("{} M in {} seconds, matches: {}", self.cnt / 1_000_000, end.as_secs() - self.start.as_secs(), self.matches)
        }
    }
}

fn parse(state: &mut State, store: bool, array_level: i32) -> JsonValue {
    let token = state.token.clone();
    state.next_token();
    match token {
        JsonTokenOwned::StartObject => {
            let mut map = HashMap::new();
            let mut is_match = false;
            while state.token != JsonTokenOwned::EndObject {
                match state.token.clone() {
                    JsonTokenOwned::JsKey(key) => {
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
        JsonTokenOwned::StartArray => {
            let mut arr = Vec::new();
            while state.token != JsonTokenOwned::EndArray {
                let obj = parse(state, store, array_level + 1);
                if store && array_level > 0 {
                    arr.push(obj);
                }
            }
            state.next_token();
            JsonValue::Array(arr)
        }
        JsonTokenOwned::JsString(value) => JsonValue::String(value),
        JsonTokenOwned::JsNumber(value) => JsonValue::Number(value),
        JsonTokenOwned::JsBoolean(value) => JsonValue::Boolean(value),
        JsonTokenOwned::JsNull => JsonValue::Null,
        _ => panic!("Invalid state"),
    }
}

fn main() -> std::io::Result<()> {
    let file = File::open("./FloridaBlue_GBO_in-network-rates.json")?;
    //let file = File::open("./layout.json")?;
    let reader = BufReader::with_capacity(1024 * 1024, file);

    let parser = JsonStream::from_read(reader, 1024 * 1024).unwrap();
    let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

    let mut state = State::new(parser);
    state.next_token();
    parse(&mut state, false, 0);

    let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    println!("{} seconds", end.as_secs() - start.as_secs());

    Ok(())
}
