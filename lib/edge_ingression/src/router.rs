use std::collections::HashMap;

use super::Service;
use super::Msg;
use super::Route;
use super::Stream;
use super::ServiceInfo;

pub struct Router<'a> {
    services: HashMap<String, Service<'a>>
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