extern crate paho_mqtt;
// Add logger

use std::thread;
use std::time::Duration;


pub struct ServiceInfo {
    pub name: String,
    pub debug: bool,
    pub host: String,
    pub protocol: Protocol
}

pub struct Protocol {
    pub name: String,
    pub port: u32,
    pub pub_topics: Vec<String>,
    pub sub_topics: Vec<String>
}

pub struct Client {
    pub name: String,
    connected: bool,
    paho: paho_mqtt::Client,
    pub service_info: ServiceInfo
}


impl Client {
    pub fn new(name: String, service_info: ServiceInfo) -> Option<Client> {
        println!("Creating new mqtt client...");

        let host = ["tcp://", &service_info.host, ":", 
                    &service_info.protocol.port.to_string()].concat();

        println!("Host: {}", host);

        let create_opts = paho_mqtt::CreateOptionsBuilder::new()
            .server_uri(host)
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
            connected: false,
            paho: paho,
            service_info: service_info
        };

        return Some(mqtt_client);
    }

    pub fn start(&mut self) -> Result<(), paho_mqtt::MqttError> {
        
        if !self.connected {
            match self.connect() {
                Ok(_) => self.connected = true,
                Err(e) => {
                    println!("Error connecting to server: {:?}", e);
                    return Err(e)
                }
            }
        }

        match self.setup_connection() {
            Ok(_) => self.connected = true,
            Err(e) => {
                println!("Error connecting to server: {:?}", e);
                return Err(e)
            }
        }

        return Ok(());
    }

    fn connect(&self) -> Result<(), paho_mqtt::MqttError> {
        let lwt = paho_mqtt::MessageBuilder::new()
            .topic("test")
            .payload("Sync consumer lost connection")
            .finalize();

        let conn_opts = paho_mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .clean_session(false)
            .will_message(lwt)
            .finalize();

        println!("Connecting to the MQTT broker...");

        if let Err(e) = self.paho.connect(conn_opts) {
            println!("Error connecting to the broker: {:?}", e);
            return Err(e);
        };

        return Ok(());
    }

    fn setup_connection(&mut self) -> Result<(), paho_mqtt::MqttError> {
        let consumer = self.paho.start_consuming();

        println!("Subscribing to topics...");
        let subscriptions = &self.service_info.protocol.sub_topics;
        let qos = [1, 1];

        if let Err(e) = self.paho.subscribe_many(&subscriptions, &qos) {
            println!("Error subscribing to topics: {:?}", e);
            println!("Disconnecting from server...");
            
            match self.paho.disconnect(None) {
                Ok(_) => {
                    println!("Disconnection successful.");
                    self.connected = false;
                    return Err(e);
                }
                Err(e) => {
                    println!("Error disconnecting from server: {:?}", e);
                    return Err(e)
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

        return Ok(());
    }

    pub fn stop(&self) -> () {

    }

    fn try_reconnect(&self) -> bool
    {
        println!("Connection lost. Waiting to retry connection");

        for _ in 0..12 {
            thread::sleep(Duration::from_millis(5000));
            if self.paho.reconnect().is_ok() {
                println!("Successfully reconnected");
                return true;
            }
        }

        println!("Unable to reconnect after several attempts.");
        false
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
            pub_topics: vec![String::from("test")],
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

// Abstract
// self.name = service_info['name']
// self.service_info = service_info
// self.streams = {}
// self.deserializer = Abstract.create_deserializer(service_info)

// Mqtt
// self.connected = False
// self.client = None
// self.queue = Queue()
// self.protocol = service_info['protocol']

// Functions
// start
// stop
// handle_msg
// add_stream
// remove_stream
// is_connected
// connect
// disconnect
// send_msg
// receive_msgs
// stop_receiving_msgs
// handle_msg
// on_mqtt_msg