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
use super::StreamInfo;
use super::ServiceInfo;

pub struct Service {
    pub name: String,
    service_info: ServiceInfo,
    streams: Arc<Mutex<HashMap<String, Stream>>>,
    client: Client,
    rx: Arc<Mutex<Receiver<Msg>>>,
}

impl Service {
    pub fn new(name: String, service_info: ServiceInfo) -> Option<Service> {
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
            streams: Arc::new(Mutex::new(HashMap::new())),
            client: client,
            rx: Arc::new(Mutex::new(rx))
        };

        return Some(mqtt_service);
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn start(&mut self) -> Result<(), ProtocolError> {
        println!("Starting service...");
        self.client.connect();
        let protocol = self.service_info.protocol.clone();
        self.client.start_subscriber(protocol);
        self.rx_msgs();

        return Ok(());
    }

    fn rx_msgs(&self) {
        let rx_clone = self.rx.clone();
        let streams_clone = self.streams.clone();

        let _service_thread = thread::spawn(move || {
            match rx_clone.lock() {
                Ok(rx) => {
                    println!("Starting service receiver thread...");
                    let mut iter = rx.iter();

                    loop {
                        let msg = iter.next();
                        println!("Service received msg: {:?}", msg);

                        match streams_clone.lock() {
                            Ok(streams) => {
                                match streams.get("temp_sensor_1") {
                                    Some(stream) => {
                                        println!(">>>>>> GOT STREAM: {:?}", stream);
                                    },
                                    None => {
                                        println!("No stream found for data");
                                    }
                                };
                            },
                            Err(_) => {
                                println!("Error locking streams");
                            }
                        }
                    }

                    Ok(())
                }
                Err(_) => {
                    println!("Error requesting receiver lock");
                    let error = ErrorKind::Thread;
                    let result = Result::Err(ProtocolError{
                        kind: error,
                        msg: "Error requesting receiver lock".to_string()
                    });
                    return result;
                }
            }

        });
    }

    pub fn stop(&self) -> Result<(), ProtocolError> {
        println!("Stopping service...");
        self.client.disconnect()
    }

    pub fn restart(&mut self) -> Result<(), ProtocolError> {
        println!("Restarting service...");

        match self.stop() {
            Ok(_) => {
                self.start()
            }
            Err(e) => Err(e)
        }
    }

    pub fn send_msg(&self, topic: Option<&str>, msg: &Msg) -> Result<(), ProtocolError> {
        match topic {
            Some(topic) => {
                self.client.send_msg(&topic.to_string(), msg)
            },
            _ => {
                self.client.send_msg(&self.service_info.protocol.pub_topic.to_string(), msg)
            },
        }
    }

    pub fn is_connected(&self) -> bool {
        self.client.is_connected()
    }

    pub fn add_stream(&mut self, stream_info: StreamInfo) -> Result<(), ProtocolError> {
        match self.streams.lock() {
            Ok(mut streams) => {
                let stream = Stream {
                    name: stream_info.name.to_string(),
                    sensor_id: stream_info.sensor_id.to_string(),
                    store_type: stream_info.store_type
                };

                println!("Adding stream: {:?} to service: {:?}", stream.sensor_id, self.name);
                streams.insert(stream.sensor_id.to_string(), stream);
                
                return Ok(())
            },
            Err(_) => {
                let error = ErrorKind::Thread;
                let result = Result::Err(ProtocolError{
                    kind: error,
                    msg: String::from("Error requesting stream lock")
                });
                return result;
            }
        }
    }

    pub fn remove_stream(&mut self, key: &str) -> Result<(), ProtocolError> {
        match self.streams.lock() {
            Ok(mut streams) => {
                streams.remove(key);
                println!("Removing stream: {:?} from service: {:?}", key, self.name);
                return Ok(())
            },
            Err(_) => {
                let error = ErrorKind::Thread;
                let result = Result::Err(ProtocolError{
                    kind: error,
                    msg: String::from("Error requesting stream lock")
                });
                return result;
            }
        }
    }

    pub fn remove_all_stream(&mut self) -> Result<(), ProtocolError> {
        match self.streams.lock() {
            Ok(mut streams) => {
                for (k, _) in streams.drain().take(1) {
                    println!("Removing stream: {:?} from service: {:?}", k, self.name);
                }
                return Ok(())
            },
            Err(_) => {
                let error = ErrorKind::Thread;
                let result = Result::Err(ProtocolError{
                    kind: error,
                    msg: String::from("Error requesting stream lock")
                });
                return result;
            }
        }
    }

    pub fn get_stream_names(&self) -> Result<Vec<String>, ProtocolError> {
        match self.streams.lock() {
            Ok(streams) => {
                let mut names = Vec::new();

                for k in streams.keys() {
                    names.push(k.clone());
                }

                return Result::Ok(names)
            },
            Err(_) => {
                let error = ErrorKind::Thread;
                let result = Result::Err(ProtocolError{
                    kind: error,
                    msg: String::from("Error requesting stream lock")
                });
                return result;
            }
        }
    }

    pub fn num_streams(&self) -> Result<usize, ProtocolError> {
        match self.streams.lock() {
            Ok(streams) => {
                

                return Result::Ok(streams.len())
            },
            Err(_) => {
                let error = ErrorKind::Thread;
                let result = Result::Err(ProtocolError{
                    kind: error,
                    msg: String::from("Error requesting stream lock")
                });
                return result;
            }
        }
    }
}