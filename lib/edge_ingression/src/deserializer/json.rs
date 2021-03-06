use super::super::Msg;

pub struct Json {

}

impl Json {
    pub fn parse_msg(&self, msg_str: &str) -> Option<Msg> {
        match serde_json::from_str::<Msg>(msg_str) {
            Ok(msg) => {
                println!("Parsing json msg =>");
                println!("\ttimestamp: {:?}", msg.timestamp);
                println!("\tversion: {:?}", msg.version);
                println!("\tdata: {:?}", msg.data);
                Some(msg)
            },
            Err(e) => {
                println!("Error parsing message: {:?}", e);
                return None;
            }
        }
    }
}