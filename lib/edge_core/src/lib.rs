use chrono::prelude::*;

// Data types
// -------------------------------------------------------------------------------------------------
#[derive(Clone, Debug)]
pub struct Event {
    pub timestamp: DateTime<Utc>
}

#[derive(Clone, Debug)]
pub enum StoreType {
    InProcessMemory,
    Redis
}

#[derive(Clone, Debug)]
pub enum DeserializerType {
    Json
}

#[derive(Clone)]
pub struct ServiceInfo {
    pub name: String,
    pub debug: bool,
    pub host: String,
    pub protocol: Protocol,
    pub deserializer: DeserializerType
}

#[derive(Clone)]
pub struct Protocol {
    pub name: String,
    pub port: u32,
    pub pub_topic: String,
    pub sub_topics: Vec<String>
}

#[derive(Clone)]
pub struct StreamInfo {
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
