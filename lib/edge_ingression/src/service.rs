use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use super::mqtt::Client;
use super::ProtocolError;
use super::ErrorKind;
use super::Msg;
use super::Stream;
use super::ServiceInfo;

#[derive(Clone)]
pub struct Service<'a> {
    pub name: String,
    service_info: ServiceInfo,
    streams: HashMap<String, &'a Stream>,
    inner: Arc<Mutex<Client>>
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