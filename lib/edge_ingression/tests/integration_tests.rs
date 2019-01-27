extern crate edge_ingression;

use edge_ingression::mqtt::client::Protocol;
use edge_ingression::mqtt::client::ServiceInfo;
use edge_ingression::mqtt::client::Client;

#[test]
fn test_mqtt_client() {
    let protocol = Protocol {
        name: String::from("mqtt"),
        port: 1883,
        pub_topic: String::from("test/"),
        sub_topics: vec![String::from("test/"), 
                         String::from("test_response")]
        
    };

    let service_info = ServiceInfo {
        name: String::from("Edge Ingestion"),
        debug: true,
        host: String::from("localhost"),
        protocol: protocol
    };

    let mut client = Client::new(String::from("test_client"), service_info).unwrap();
    let result = client.start();
    let topic = "test/";
    let msg = "Test msg";
    client.send_msg(topic, msg);
    loop {}
}