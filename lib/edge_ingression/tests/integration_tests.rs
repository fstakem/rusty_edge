extern crate edge_ingression;

use edge_ingression::mqtt::client::Protocol;
use edge_ingression::mqtt::client::ServiceInfo;
use edge_ingression::mqtt::client::Msg;
use edge_ingression::mqtt::client::MsgType;
use edge_ingression::mqtt::client::SensorData;
use edge_ingression::mqtt::client::Stream;
use edge_ingression::mqtt::client::StoreType;
use edge_ingression::mqtt::client::Router;


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
        protocol: protocol
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

    let sensor_data = SensorData {
        sensor_id: String::from("Remote sensor a"),
        data: vec![10.0, 12.0]
    };

    let data_value = serde_json::json!(&sensor_data);

    let msg = Msg {
        timestamp: "".to_string(),
        version: "0.1.0".to_string(),
        msg_type: MsgType::SensorData,
        data: data_value  
    };

    router.send_msg(service_name.as_str(), topic, &msg);
    loop {}
}