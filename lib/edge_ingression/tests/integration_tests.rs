extern crate edge_ingression;

use edge_ingression::mqtt::client::Protocol;
use edge_ingression::mqtt::client::ServiceInfo;
use edge_ingression::mqtt::client::Client;
use edge_ingression::mqtt::client::Msg;

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
    let other = Msg {
        timestamp: "".to_string(),
        version: "0.1.0".to_string(),
        msg_type: "simple".to_string(),
        data: "here".to_string()   
    };

    client.send_msg(Some(topic), &other);
    loop {}
}