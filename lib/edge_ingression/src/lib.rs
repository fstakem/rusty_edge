#[macro_use]
extern crate serde_derive;
extern crate paho_mqtt;
extern crate serde;
extern crate serde_json;
extern crate chrono;
extern crate edge_core;
extern crate edge_data_store;

use chrono::prelude::*;

pub mod protocol;
pub mod deserializer;
pub mod router;
pub mod service;

pub use self::router::Router;
pub use self::service::Service;


// Data types
// -------------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct Route {
    pub service_name: String,
    pub stream_name: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Msg {
    pub timestamp: DateTime<Utc>,
    pub version: String,
    pub data: MsgData
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "msg_type")]
pub enum MsgData {
    #[serde(rename = "simple_data")]
    SimpleData {values: Vec<f64> },
    #[serde(rename = "descriptive_data")]
    DescriptiveData {ids: Vec<String>, values: Vec<f64> },
    #[serde(rename = "window_data")]
    WindowData {timestamps: Vec<DateTime<Utc>>, values: Vec<f64> },
    #[serde(rename = "other")]
    Other { value: String },
}

#[derive(Debug)]
pub struct ProtocolError {
    pub kind: ErrorKind,
    pub msg: String
}

#[derive(Debug)]
pub enum ErrorKind {
    General,
    Thread,
    Mqtt,
}

#[derive(Debug)]
pub struct Stream {
    pub name: String,
    pub sensor_id: String,
    //pub store: T
}


// Tests
// -------------------------------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
