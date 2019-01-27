extern crate paho_mqtt;

use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

#[derive(Clone)]
pub struct ServiceInfo {
    pub name: String,
    pub debug: bool,
    pub host: String,
    pub protocol: Protocol
}

// deserializer
// schema

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
    inner: Arc<Mutex<InnerClient>>
}

impl InnerClient {
    fn do_something(&mut self) -> () {
        println!("In thread");
    }

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

    fn run_subscriber(&mut self, protocol: Protocol) -> Result<(), ProtocolError> {
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
                println!("{}", msg);
            }
            else if self.paho.is_connected() ||
                    !self.try_reconnect() {
                break;
            }
        }

        println!("Finished subscriber.");

        return Ok(());
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
               Ok(mut client) => client.run_subscriber(protocol),
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

    pub fn stop(&self) -> () {
        println!("Mqtt client stopping...");
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