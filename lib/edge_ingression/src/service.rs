use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::sync::mpsc::{Receiver, channel};

use super::protocol::mqtt::Client;
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
    client: Arc<Mutex<Client>>,
    rx: Arc<Mutex<Receiver<Msg>>>,
}

impl<'a> Service<'a>{
    pub fn new(name: String, service_info: ServiceInfo) -> Option<Service<'a>> {
        println!("Creating new service...");

        let (tx, rx) = channel();

        let client = match Client::new(&service_info, tx) {
            Some(client) => client,
            None => {
                return None
            },
        };

        let mqtt_service = Service {
            name:  name,
            service_info: service_info,
            streams: HashMap::new(),
            client: Arc::new(Mutex::new(client)),
            rx: Arc::new(Mutex::new(rx))
        };

        return Some(mqtt_service);
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn start(&mut self) -> Result<(), ProtocolError> {
        println!("Starting mqtt service...");

        match self.client.lock() {
            Ok(mut client) => {
                client.connect();
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

        let client_clone = self.client.clone();
        let protocol = self.service_info.protocol.clone();

        let paho_thread = thread::spawn(move || {
            match client_clone.lock() {
               Ok(mut client) => client.start_subscriber(protocol),
               Err(_) => {
                    println!("Error requesting client lock");
                    let error = ErrorKind::Thread;
                    let result = Result::Err(ProtocolError{
                        kind: error,
                        msg: "Error starting client".to_string()
                    });
                    return result;
               }
            }
        });

        let rx_clone = self.rx.clone();

        let service_thread = thread::spawn(move || {
            match rx_clone.lock() {
                Ok(rx) => {
                    println!("Starting service thread...");
                    let mut iter = rx.iter();

                    loop {
                        println!("waiting.......");
                        let msg = iter.next();
                        println!("Service received: {:?}", msg);
                    }

                    Ok(())
                }
                Err(_) => {
                    println!("Error requesting receiver lock");
                    let error = ErrorKind::Thread;
                    let result = Result::Err(ProtocolError{
                        kind: error,
                        msg: "Error starting client".to_string()
                    });
                    return result;
                }
            }

        });

        return Ok(());
    }

    pub fn stop(&self) -> Result<(), ProtocolError> {
        println!("Stopping mqtt service...");

        match self.client.lock() {
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
        println!("++++++++++++++++++++++-> Service trying to get lock");
        match self.client.lock() {
            Ok(client) => {
                if let Some(new_topic) = topic {
                    println!("++++++++++++++++++++++-> Service sending msg");
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

    fn receive_msgs(&self) {

    }

    pub fn is_connected(&self) -> Result<(bool), ProtocolError> {
        match self.client.lock() {
            Ok(client) => {
                return Result::Ok(client.is_connected());
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