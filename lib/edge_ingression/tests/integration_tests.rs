extern crate edge_ingression;

use chrono::prelude::*;

use edge_core::Protocol;
use edge_core::ServiceInfo;
use edge_core::DeserializerType;
use edge_core::StreamInfo;
use edge_core::StoreType;
use edge_ingression::Msg;
use edge_ingression::Router;
use edge_ingression::MsgData;



fn send_simple_data(router: &Router, service_name: &str, topic: &str) {
    let sensor_data = MsgData::SimpleData{ values: vec![10.0, 12.0] };
    let utc: DateTime<Utc> = Utc::now();

    let msg = Msg {
        timestamp: utc,
        version: "0.1.0".to_string(),
        data: sensor_data  
    };

    router.send_msg(service_name, topic, &msg);
}

fn send_descriptive_data(router: &Router, service_name: &str, topic: &str) {
    let sensor_data = MsgData::DescriptiveData{ ids: vec![String::from("a"), String::from("b")], 
                                                values: vec![10.0, 12.0] };
    let utc: DateTime<Utc> = Utc::now();

    let msg = Msg {
        timestamp: utc,
        version: "0.1.0".to_string(),
        data: sensor_data  
    };

    router.send_msg(service_name, topic, &msg);
}

fn send_window_data(router: &Router, service_name: &str, topic: &str) {
    let sensor_data = MsgData::WindowData{ timestamps: vec![Utc::now(), Utc::now()], 
                                           values: vec![10.0, 12.0] };
    let utc: DateTime<Utc> = Utc::now();

    let msg = Msg {
        timestamp: utc,
        version: "0.1.0".to_string(),
        data: sensor_data  
    };

    router.send_msg(service_name, topic, &msg);
}


#[test]
fn test_mqtt_service() {
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
        protocol: protocol,
        deserializer: DeserializerType::Json
    };

    let simple_stream = StreamInfo {
        name: String::from("Temp sensor"),
        sensor_id: String::from("temp_sensor_1"),
        store_type: StoreType::InProcessMemory
    };

    let mut router = Router::new();
    let service_name = service_info.name.clone();
    router.add_service(service_info);
    let route = router.add_route(service_name.as_str(), simple_stream);
    println!("Route: {:?}", route);
    router.start();

    let topic = "test/";
    println!("Msg #1");
    send_simple_data(&router, service_name.as_str(), topic);
    println!("");

    println!("Msg #2");
    send_descriptive_data(&router, service_name.as_str(), topic);
    println!("");

    println!("Msg #3");
    send_window_data(&router, service_name.as_str(), topic);
    println!("");

    loop {}
}