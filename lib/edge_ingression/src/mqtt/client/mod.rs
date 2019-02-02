extern crate paho_mqtt;
extern crate serde;
extern crate serde_json;
extern crate serde_derive;

use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use std::collections::HashMap;

pub struct Stream {
    pub name: String,
    pub sensor_id: String,
    pub store_type: String
}

#[derive(Clone)]
pub struct ServiceInfo {
    pub name: String,
    pub debug: bool,
    pub host: String,
    pub protocol: Protocol
}

// deserializer
// schema

// self.name = service_info['name']
// self.schema = service_info['schema']
// self.checked = True
// self.types = {}
// self.parsers = {}
// self.fields = []
// self.event_class = None

#[derive(Clone)]
pub struct Protocol {
    pub name: String,
    pub port: u32,
    pub pub_topic: String,
    pub sub_topics: Vec<String>
}

struct InnerClient {
    paho: paho_mqtt::Client,
    connected: bool
}

pub struct Client {
    pub name: String,
    pub service_info: ServiceInfo,
    pub streams: HashMap<String, Stream>,
    inner: Arc<Mutex<InnerClient>>
}

unsafe impl Send for InnerClient {}

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


impl InnerClient {
    fn connect(&mut self) -> Result<(), ProtocolError> {
        if !self.connected {
            let lwt = paho_mqtt::Message::new("test", "Sync subscriber lost connection", 1);

            let conn_opts = paho_mqtt::ConnectOptionsBuilder::new()
                .keep_alive_interval(Duration::from_secs(20))
                .clean_session(false)
                .will_message(lwt)
                .finalize();

            println!("Connecting to the MQTT broker...");

            match self.paho.connect(conn_opts) {
                Ok(_) => self.connected = true,
                Err(e) => {
                    println!("Error connecting to server: {:?}", e);
                    let error = ErrorKind::Mqtt;
                    let result = Result::Err(ProtocolError{
                        kind: error,
                        msg: "Error connecting to server".to_string()
                    });
                    return result;
                }
            }

             println!("Connection successful!");

            return Ok(());
        }

        println!("Already connected to the server.");

        return Ok(());
    }

    fn start_subscriber(&mut self, protocol: Protocol) -> Result<(), ProtocolError> {
        let consumer = self.paho.start_consuming();

        println!("Subscribing to topics...");
        let subscriptions = protocol.sub_topics;
        let qos = [1, 1];

         if let Err(e) = self.paho.subscribe_many(&subscriptions, &qos) {
            println!("Error subscribing to topics: {:?}", e);
            println!("Disconnecting from server...");
            
            match self.paho.disconnect(None) {
                Ok(_) => {
                    println!("Disconnection successful.");
                    self.connected = false;
                    let error = ErrorKind::Mqtt;
                    return Result::Err(ProtocolError{
                        kind: error,
                        msg: "Error subscribing to topics".to_string()
                    });
                }
                Err(e) => {
                    println!("Error disconnecting from server: {:?}", e);
                    let error = ErrorKind::Mqtt;
                    return Result::Err(ProtocolError{
                        kind: error,
                        msg: "Error subscribing to topics".to_string()
                    });
                }
            }
        }

        println!("Waiting for messages...");

        for msg in consumer.iter() {
            if let Some(msg) = msg {
                self.handle_msg(msg);
            }
            else if self.paho.is_connected() ||
                    !self.try_reconnect() {
                break;
            }
        }

        println!("Finished subscriber.");

        return Ok(());
    }

    fn handle_msg(&self, msg: paho_mqtt::Message) -> () {
        // Handle in stream
        println!("{}", msg);

        let outer_msg = match serde_json::from_str::<Msg>(&msg.payload_str()) {
            Ok(outer_msg) => {
                println!("Message received =>");
                println!("\ttimestamp: {:?}", outer_msg.timestamp);
                println!("\tversion: {:?}", outer_msg.version);
                println!("\tmessage type: {:?}", outer_msg.msg_type);
                println!("\tdata: {:?}", outer_msg.data);
                outer_msg
            },
            Err(e) => {
                println!("Error parsing message: {:?}", e);
                return ();
            }
        };

    }

    fn try_reconnect(&self) -> bool {
        println!("Connection lost. Waiting to retry connection");

        for _ in 0..12 {
            thread::sleep(Duration::from_millis(5000));

             if self.paho.reconnect().is_ok() {
                println!("Successfully reconnected");
                return true;
            }
        }

        println!("Unable to reconnect after several attempts.");
        return false;
    }

    fn send_msg(&self, topic: &str, msg: &Msg) -> Result<(), ProtocolError> {
        println!("Sending an mqtt msg...");

        let msg_str = match serde_json::to_string(&msg) {
            Ok(msg_str) => {
                println!("{:?}", msg_str);
                msg_str
            },
            Err(e) => {
                let error = ErrorKind::Mqtt;
                let result = Result::Err(ProtocolError{
                    kind: error,
                    msg: "Error publishing message".to_string()
                });
                return result;
            }
        };

        if self.connected {
            let mqtt_msg = paho_mqtt::MessageBuilder::new()
                .topic(topic)
                .payload(msg_str.clone())
                .qos(1)
                .finalize();

            if let Err(e) = self.paho.publish(mqtt_msg) {
                let error = ErrorKind::Mqtt;
                let result = Result::Err(ProtocolError{
                    kind: error,
                    msg: "Error publishing message".to_string()
                });
                return result;
            }

            let output = ["Topic: ", &topic, " Msg: ", &msg_str].concat();
            println!("{}", output);

            return Ok(());
        }

        let error = ErrorKind::Mqtt;
        let result = Result::Err(ProtocolError{
            kind: error,
            msg: "Error not connected".to_string()
        });
        return result;
    }

    fn disconnect(&self) -> Result<(), ProtocolError> {
        if self.connected {
            self.paho.disconnect(None);
            return Result::Ok(());
        }

        let error = ErrorKind::Mqtt;
        let result = Result::Err(ProtocolError{
            kind: error,
            msg: "Error not connected".to_string()
        });
        return result;
    }
}


impl Client {
    pub fn new(name: String, service_info: ServiceInfo) -> Option<Client> {
        println!("Creating new mqtt client...");

        let conn_str = ["tcp://", &service_info.host, ":", 
                    &service_info.protocol.port.to_string()].concat();

        println!("Connection string: {}", conn_str);

        let create_opts = paho_mqtt::CreateOptionsBuilder::new()
            .server_uri(conn_str)
            .client_id("rust_sync_consumer")
            .finalize();

        let paho = match paho_mqtt::Client::new(create_opts) {
            Ok(paho) => paho,
            Err(e) => {
                println!("Error creating the client: {:?}", e);
                return None
           },
        };

        let mqtt_client = Client {
            name:  name,
            service_info: service_info,
            streams: HashMap::new(),
            inner: Arc::new( Mutex::new( InnerClient {
                paho: paho,
                connected: false
                
            }))
        };

        return Some(mqtt_client);
    }

    pub fn start(&mut self) -> Result<(), ProtocolError> {
        println!("Mqtt client starting...");

        match self.inner.lock() {
            Ok(mut client) => {
                client.connect();
            }
            Err(e) => {
                println!("Error requesting lock");
                let error = ErrorKind::Thread;
                let result = Result::Err(ProtocolError{
                    kind: error,
                    msg: "Error requesting lock".to_string()
                });
                return result;
            }
        }

        let local_self = self.inner.clone();
        let protocol = self.service_info.protocol.clone();

        let child = thread::spawn(move || {
            match local_self.lock() {
               Ok(mut client) => client.start_subscriber(protocol),
               Err(e) => {
                    println!("Error requesting lock");
                    let error = ErrorKind::Thread;
                    let result = Result::Err(ProtocolError{
                        kind: error,
                        msg: "Error connecting to server".to_string()
                    });
                    return result;
               }
            }
        });

        return Ok(());
    }

    pub fn stop(&self) -> Result<(), ProtocolError> {
        println!("Mqtt client stopping...");


        match self.inner.lock() {
            Ok(client) => {
                return client.disconnect();
            }
            Err(e) => {
                println!("Error requesting lock");
                let error = ErrorKind::Thread;
                let result = Result::Err(ProtocolError{
                    kind: error,
                    msg: "Error requesting lock".to_string()
                });
                return result;
            }
        }
    }

    pub fn send_msg(&self, topic: Option<&str>, msg: &Msg) -> Result<(), ProtocolError> {
        match self.inner.lock() {
            Ok(client) => {

                if let Some(new_topic) = topic {
                    return client.send_msg(new_topic, msg);
                }

                return client.send_msg(&self.service_info.protocol.pub_topic.to_string(), msg);
            }
            Err(e) => {
                println!("Error requesting lock");
                let error = ErrorKind::Thread;
                let result = Result::Err(ProtocolError{
                    kind: error,
                    msg: "Error requesting lock".to_string()
                });
                return result;
            }
        }
    }

    pub fn is_connected(&self) -> Result<(bool), ProtocolError> {
        match self.inner.lock() {
            Ok(client) => {
                return Result::Ok(client.connected);
            }
            Err(e) => {
                println!("Error requesting lock");
                let error = ErrorKind::Thread;
                let result = Result::Err(ProtocolError{
                    kind: error,
                    msg: "Error requesting lock".to_string()
                });
                return result;
            }
        }
    }

    pub fn add_stream(&mut self, key: &str, stream: Stream) -> Result<(), ProtocolError> {
        self.streams.insert(key.to_string(), stream);
        return Result::Ok(());
    }

    pub fn remove_stream(&self) -> Result<(), ProtocolError> {
        return Result::Ok(());
    }

    pub fn remove_all_stream(&self) -> Result<(), ProtocolError> {
        return Result::Ok(());
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let protocol = Protocol {
            name: String::from("mqtt"),
            port: 1883,
            pub_topic: String::from("test"),
            sub_topics: vec![String::from("test"), 
                             String::from("test_response")]
            
        };

        let service_info = ServiceInfo {
            name: String::from("Edge Ingestion"),
            debug: true,
            host: String::from("localhost"),
            protocol: protocol
        };

        let client = Client::new(String::from("test_client"), service_info).unwrap();
        assert_eq!(client.name, String::from("test_client"));
    }
}