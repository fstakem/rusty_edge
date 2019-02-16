use std::thread;
use std::time::Duration;

use super::super::ProtocolError;
use super::super::ErrorKind;
use super::super::Protocol;
use super::super::Msg;
use super::super::SensorData;
use super::super::MsgType;
use super::super::ServiceInfo;


pub struct Client {
    paho: paho_mqtt::Client,
    pub connected: bool
}


unsafe impl Send for Client {}


impl Client {
    pub fn new(service_info: &ServiceInfo) -> Option<Client> {
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

        let client = Client {
            paho: paho,
            connected: false
        };

        Some(client)
    }

    pub fn connect(&mut self) -> Result<(), ProtocolError> {
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

    pub fn start_subscriber(&mut self, protocol: Protocol) -> Result<(), ProtocolError> {
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
        // TODO
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

        match outer_msg.msg_type {
            MsgType::SensorData => {
                match serde_json::from_value::<SensorData>(outer_msg.data) {
                    Ok(data) => {
                        println!("Sensor data =>");
                        println!("\tsensor id: {:?}", data.sensor_id);
                        println!("\tdata: {:?}", data.data)
                    },
                    Err(e) => {
                        println!("Error parsing sensor data: {:?}", e);
                        return ();
                    }
                }
            },
            _ => {
                println!("Msg type not handled!")
            }
        }

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

    pub fn send_msg(&self, topic: &str, msg: &Msg) -> Result<(), ProtocolError> {
        println!("Sending an mqtt msg...");

        let msg_str = match serde_json::to_string(&msg) {
            Ok(msg_str) => {
                println!("{:?}", msg_str);
                msg_str
            },
            Err(_) => {
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

            if let Err(_) = self.paho.publish(mqtt_msg) {
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

    pub fn disconnect(&self) -> Result<(), ProtocolError> {
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


#[cfg(test)]
mod tests {
    use super::*;
}