extern crate edge_ingression;

use chrono::prelude::*;

use edge_ingression::Protocol;
use edge_ingression::ServiceInfo;
use edge_ingression::Msg;
use edge_ingression::Stream;
use edge_ingression::StoreType;
use edge_ingression::Router;
use edge_ingression::MsgData;


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
        deserializer: String::from("json")
    };

    let simple_stream = Stream {
        name: String::from("Temp sensor"),
        sensor_id: String::from("temp_sensor_1"),
        store_type: StoreType::InProcessMemory
    };

    let mut router = Router::new();
    let service_name = service_info.name.clone();
    router.add_service(service_info);
    let route = router.add_route(service_name.as_str(), &simple_stream);
    println!("Route: {:?}", route);
    router.start();

    let topic = "test/";
    let sensor_data = MsgData::SimpleData{ values: vec![10.0, 12.0] };
    let utc: DateTime<Utc> = Utc::now();

    let msg = Msg {
        timestamp: utc,
        version: "0.1.0".to_string(),
        data: sensor_data  
    };

    router.send_msg(service_name.as_str(), topic, &msg);
    loop {}
}