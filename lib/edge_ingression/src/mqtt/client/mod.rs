extern crate paho_mqtt;
extern crate serde;
extern crate serde_json;
extern crate serde_derive;

use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use std::collections::HashMap;


pub enum StoreType {
    InProcessMemory,
    Redis
}

pub struct Stream {
    pub name: String,
    pub sensor_id: String,
    pub store_type: StoreType
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

pub struct Router<'a> {
    services: HashMap<String, Service<'a>>
}

#[derive(Clone)]
pub struct Protocol {
    pub name: String,
    pub port: u32,
    pub pub_topic: String,
    pub sub_topics: Vec<String>
}

struct Client {
    paho: paho_mqtt::Client,
    connected: bool
}

#[derive(Clone)]
pub struct Service<'a> {
    pub name: String,
    service_info: ServiceInfo,
    streams: HashMap<String, &'a Stream>,
    inner: Arc<Mutex<Client>>
}

unsafe impl Send for Client {}

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


impl<'a> Router<'a> {
    pub fn new() -> Router<'a> {
        let router = Router {
            services: HashMap::new()
        };

        return router
    }

    pub fn get_route_names(&self) -> Vec<String> {
        let mut route_names = Vec::<String>::new();

        for (_, service) in self.services.iter() {
            let stream_names = service.get_stream_names();

            for n in stream_names.iter() {
                let mut route_name = String::new();
                route_name.push_str(service.name.as_str());
                route_name.push_str("_");
                route_name.push_str(n);
                route_names.push(route_name);
            }
        }

        return route_names
    }

    pub fn add_route(&mut self, service_name: &str, stream: & 'a Stream) -> Option<Route> {
        match self.services.get_mut(service_name) {
            Some(service) => {
                println!("Adding stream: {:?} to service: {:?}", stream.name, service_name);
                
                match service.add_stream(stream) {
                    Ok(_) => {
                        let route = Route {
                            service_name: service_name.to_string(),
                            stream_name: stream.name.to_string()
                        };

                        Some(route)
                    },
                    Err(_) => {
                        None
                    }
                }
            },
            _ => {
                println!("Service not found: {:?}", service_name);
                None
            },
        }
    }

    pub fn remove_route(&mut self, service_name: &str, stream_name: &str) -> Option<Route> {
        match self.services.get_mut(service_name) {
            Some(service) => {
                println!("Removing stream: {:?} from service: {:?}", stream_name, service_name);

                match service.remove_stream(stream_name) {
                    Ok(_) => {
                        let route = Route {
                            service_name: service_name.to_string(),
                            stream_name:stream_name.to_string()
                        };

                        Some(route)
                    },
                    Err(_) => {
                        None
                    }
                }
            },
            _ => {
                println!("Service not found: {:?}", service_name);
                None
            },
        }
    }

    pub fn num_routes(&self) -> usize {
        let mut total = 0;

        for (_, service) in self.services.iter() {
            total += service.num_streams();
        }

        return total
    }

    pub fn get_service_names(&self) -> Vec<String> {
        let mut service_names = Vec::<String>::new();

        for (name, _) in self.services.iter() {
            service_names.push(name.to_string());
        }

        return service_names
    }

    pub fn add_service(&mut self, service_info: ServiceInfo) {
        match self.services.get(&service_info.name) {
            Some(_) => {
                println!("Service already exists: {:?}", service_info.name);
            },
            _ => {
                println!("Creating service: {:?}", service_info.name);
                let key = service_info.name.clone();

                match Service::new(service_info.name.clone(), service_info) {
                    Some(service) => {
                        println!("Service created");
                        self.services.insert(key, service);
                    },
                    None => {
                        println!("Error creating service")
                    }
                }
            },
        }
    }

    pub fn remove_service(&mut self, service_name: &str) {
        match self.services.remove(service_name) {
            Some(_) => {
                println!("Removing service: {:?}", service_name);
            },
            _ => {
                println!("Service not found: {:?}", service_name)
            },
        }
    }

    pub fn num_services(&self) -> usize {
        return self.services.len()
    }

    pub fn start(&mut self) {
        for (_, service) in self.services.iter_mut() {
            service.start();
        }
    }

    pub fn send_msg(&self, service_name: &str, topic: &str, msg: &Msg) {
        match self.services.get(service_name) {
            Some(service) => {
                service.send_msg(Some(topic), msg);
            },
            _ => {
                println!("Service does not exists: {:?}", service_name);
            }
        }
    }
}


impl Client {
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

    fn send_msg(&self, topic: &str, msg: &Msg) -> Result<(), ProtocolError> {
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


impl<'a> Service<'a>{
    pub fn new(name: String, service_info: ServiceInfo) -> Option<Service<'a>> {
        println!("Creating new mqtt service...");

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

        let mqtt_service = Service {
            name:  name,
            service_info: service_info,
            streams: HashMap::new(),
            inner: Arc::new( Mutex::new( Client {
                paho: paho,
                connected: false
                
            }))
        };

        return Some(mqtt_service);
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn start(&mut self) -> Result<(), ProtocolError> {
        println!("Starting mqtt service...");

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
        println!("Stopping mqtt service...");

        match self.inner.lock() {
            Ok(client) => {
                return client.disconnect();
            }
            Err(_) => {
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

    pub fn restart(&mut self) -> Result<(), ProtocolError> {
        println!("Restarting mqtt service...");
        match self.stop() {
            Ok(_) => {
                self.start()
            }
            Err(e) => Err(e)
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
            Err(_) => {
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
            Err(_) => {
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

    pub fn add_stream(&mut self, stream: &'a Stream) -> Result<(), ProtocolError> {
        self.streams.insert(stream.name.to_string(), stream);
        println!("Adding stream: {:?} to service: {:?}", stream.name, self.name);
        return Ok(())
    }

    pub fn remove_stream(&mut self, key: &str) -> Result<(), ProtocolError> {
        self.streams.remove(key);
        println!("Removing stream: {:?} from service: {:?}", key, self.name);
        return Ok(())
    }

    pub fn remove_all_stream(&mut self) -> Result<(), ProtocolError> {
        for (k, _) in self.streams.drain().take(1) {
            println!("Removing stream: {:?} from service: {:?}", k, self.name);
        }
        return Ok(())
    }

    pub fn get_stream_names(&self) -> Vec<&str> {
        let mut names = Vec::new();

        for k in self.streams.keys() {
            names.push(k.as_str());
        }

        return names
    }

    pub fn num_streams(&self) -> usize {
        self.streams.len()
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

        let service = Service::new(String::from("test_service"), service_info).unwrap();
        assert_eq!(service.name, String::from("test_service"));
    }
}