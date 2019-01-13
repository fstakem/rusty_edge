extern crate paho_mqtt;

use std::process;
use std::thread;
use std::time::Duration;

pub struct Client {
    name: String,
    connected: bool,
    client: paho_mqtt::Client
}


impl Client {
    pub fn new(name: String) -> Client {
        println!("Creating new mqtt client...");

        let host = "tcp://localhost:1883";
        let create_opts = paho_mqtt::CreateOptionsBuilder::new()
            .server_uri(host)
            .client_id("rust_sync_consumer")
            .finalize();
        let client = paho_mqtt::Client::new(create_opts).unwrap_or_else(|e| {
            println!("Error creating the client: {:?}", e);
            process::exit(1);
        });

        let mqtt_client = Client {
            name:  name,
            connected: false,
            client: client
        };

        return mqtt_client;
    }

    pub fn start(&mut self) -> () {
        // Define the set of options for the connection
        let lwt = paho_mqtt::MessageBuilder::new()
            .topic("test")
            .payload("Sync consumer lost connection")
            .finalize();

        let conn_opts = paho_mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .clean_session(false)
            .will_message(lwt)
            .finalize();

        // Make the connection to the broker
        println!("Connecting to the MQTT broker...");
        if let Err(e) = self.client.connect(conn_opts) {
            println!("Error connecting to the broker: {:?}", e);
            process::exit(1);
        };

        // Initialize the consumer before subscribing to topics
        let rx = self.client.start_consuming();

        // Register subscriptions on the server
        println!("Subscribing to topics...");

        let subscriptions = [ "test", "hello" ];
        let qos = [1, 1];

        if let Err(e) = self.client.subscribe_many(&subscriptions, &qos) {
            println!("Error subscribing to topics: {:?}", e);
            self.client.disconnect(None).unwrap();
            process::exit(1);
        }

        println!("Waiting for messages...");
        for msg in rx.iter() {
            if let Some(msg) = msg {
                println!("{}", msg);
            }
            else if self.client.is_connected() ||
                    !self.try_reconnect() {
                break;
            }
        }
    }

    pub fn stop(&self) -> () {

    }

    fn try_reconnect(&self) -> bool
    {
        println!("Connection lost. Waiting to retry connection");
        for _ in 0..12 {
            thread::sleep(Duration::from_millis(5000));
            if self.client.reconnect().is_ok() {
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
        let client = Client::new("client".to_string());
        assert_eq!(client.name, "client".to_string());
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