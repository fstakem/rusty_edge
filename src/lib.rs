extern crate edge_ingression as ingression;

pub fn mqtt_start() -> () {
    ingression::mqtt::client::Client::new("test".to_string());
}