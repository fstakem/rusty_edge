use std::time::Duration;
use std::sync::mpsc::Sender;

use super::super::super::ProtocolError;
use super::super::super::ErrorKind;
use super::super::super::Msg;
use super::super::super::deserializer::json::Json;
use edge_core::Protocol;
use edge_core::DeserializerType;
use edge_core::ServiceInfo;


pub struct Client {
    paho: paho_mqtt::AsyncClient
}

unsafe impl Send for Client {}

fn on_connect_success(_paho_client: &paho_mqtt::AsyncClient, _msgid: u16) {
    println!("Connection to MQTT broker succeeded");
}

fn on_connect_failure(_paho_client: &paho_mqtt::AsyncClient, _msgid: u16, rc: i32) {
    println!("Connection to MQTT broker failed with error code: {}", rc);
}


impl Client {
    pub fn new(service_info: &ServiceInfo, transmitter: Sender<Msg>) -> Option<Client> {
        println!("Creating new MQTT client...");

        let conn_str = ["tcp://", &service_info.host, ":", 
                    &service_info.protocol.port.to_string()].concat();

        println!("MQTT connection string: {}", conn_str);

        let create_opts = paho_mqtt::CreateOptionsBuilder::new()
            .server_uri(conn_str)
            .client_id("rust_async_consumer")
            .finalize();

        let paho = match paho_mqtt::AsyncClient::new(create_opts) {
            Ok(paho) => paho,
            Err(e) => {
                println!("Error creating the MQTT client: {:?}", e);
                return None
           },
        };

        let mut client = Client {
            paho: paho
        };

        client.paho.set_connection_lost_callback(|_paho_client: &paho_mqtt::AsyncClient| {
            println!("Connection lost to the MQTT broker");
        });

        let deserializer = match service_info.deserializer {
            DeserializerType::Json => { Json{} },
            _ => return None
        };

        client.paho.set_message_callback(move |_paho_client, msg| {
            if let Some(msg) = msg {
                let topic = msg.topic();
                let payload_str = msg.payload_str();
                println!("MQTT msg topic: {} data: {}", topic, payload_str);

                match deserializer.parse_msg(&payload_str) {
                    Some(msg) => {
                        transmitter.send(msg);
                    },
                    None => {
                        
                    }
                }
            }
        });
        
        Some(client)
    }

    pub fn connect(&mut self) -> Result<(), ProtocolError> {
        if !self.paho.is_connected() {
            let lwt = paho_mqtt::Message::new("test", "Lost connect to MQTT broker", 1);

            let conn_opts = paho_mqtt::ConnectOptionsBuilder::new()
                .keep_alive_interval(Duration::from_secs(20))
                .clean_session(false)
                .will_message(lwt)
                .finalize();

            println!("Connecting to the MQTT broker...");
            let result = self.paho.connect_with_callbacks(conn_opts, on_connect_success, on_connect_failure);

            if let Err(e) = result.wait() {
                println!("Error connecting to MQTT broker: {:?}", e);
            }

            return Ok(());
        }

        println!("Already connected to the MQTT broker");

        return Ok(());
    }

    pub fn start_subscriber(&mut self, protocol: Protocol) -> Result<(), ProtocolError> {
        println!("Subscribing to MQTT topics...");
        let subscriptions = protocol.sub_topics;
        let qos = [1, 1];
        self.paho.subscribe_many(&subscriptions, &qos);
        println!("MQTT client waiting for messages...");

        return Ok(());
    }

    pub fn send_msg(&self, topic: &str, msg: &Msg) -> Result<(), ProtocolError> {
        println!("MQTT client sending a msg...");

        let msg_str = match serde_json::to_string(&msg) {
            Ok(msg_str) => {
                println!("Msg: {:?}", msg_str);
                msg_str
            },
            Err(_) => {
                let error = ErrorKind::Mqtt;
                let result = Result::Err(ProtocolError{
                    kind: error,
                    msg: "Error publishing MQTT message".to_string()
                });
                return result;
            }
        };

        if self.paho.is_connected() {
            let mqtt_msg = paho_mqtt::MessageBuilder::new()
                .topic(topic)
                .payload(msg_str.clone())
                .qos(0)
                .finalize();

            let output = ["Topic: ", &topic, " Msg: ", &msg_str].concat();
            println!("MQTT publishing: {}", output);
            let tok = self.paho.publish(mqtt_msg);

            if let Err(e) = tok.wait() {
                println!("Error sending message: {:?}", e);
            }

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
        println!("Attempting to disconnect from MQTT broker...");
        if self.paho.is_connected() {
            self.paho.disconnect(None);
            return Result::Ok(());
        }

        println!("Already disconnected from the MQTT broker");

        return Ok(());
    }

    pub fn is_connected(&self) -> bool {
        self.paho.is_connected()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
}