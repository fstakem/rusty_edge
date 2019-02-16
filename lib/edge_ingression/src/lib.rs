#[macro_use]
extern crate serde_derive;
extern crate paho_mqtt;
extern crate serde;
extern crate serde_json;

pub mod mqtt;
pub mod router;
pub mod service;

pub use self::router::Router;
pub use self::service::Service;


// Data types
// -------------------------------------------------------------------------------------------------
pub enum StoreType {
    InProcessMemory,
    Redis
}

#[derive(Clone)]
pub struct ServiceInfo {
    pub name: String,
    pub debug: bool,
    pub host: String,
    pub protocol: Protocol
}

#[derive(Debug)]
pub struct Route {
    pub service_name: String,
    pub stream_name: String
}

#[derive(Clone)]
pub struct Protocol {
    pub name: String,
    pub port: u32,
    pub pub_topic: String,
    pub sub_topics: Vec<String>
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MsgType {
    SensorData,
    Config,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Msg {
    pub timestamp: String,
    pub version: String,
    pub msg_type: MsgType,
    pub data: serde_json::Value
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SensorData {
    pub sensor_id: String,
    pub data: Vec<f64>
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

pub struct Stream {
    pub name: String,
    pub sensor_id: String,
    pub store_type: StoreType
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
