use kstream::Config;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::admin::AdminOptions;

use log::{*};

pub fn cfg() -> Config {
    Config::new()
        .set("bootstrap.servers", "localhost:29092")
        .set("auto.offset.reset", "earliest")
}

pub fn init() {
    std::env::set_var("RUST_LOG", "rdkafka=debug,trace");
    env_logger::init();
}

pub async fn clear_topics() {
    let config = cfg();
    let admin = rdkafka::admin::AdminClient::from_config(&config.0);
    let admin = admin.unwrap();

    let con = BaseConsumer::from_config(&config.0).unwrap();
    let topics = con.fetch_metadata(None, None);
    for t in topics.unwrap().topics() {
        let r = admin.delete_topics(&[t.name()], &AdminOptions::new()).await.unwrap();
        for r in r {
            let r = r.unwrap();
            info!("Deleted topic {:?} -> {:?}", t.name(), r);
        }
    }
}

pub fn get_topics() -> Vec<String> {
    let con = BaseConsumer::from_config(&cfg().0).unwrap();
    let topics = con.fetch_metadata(None, None).unwrap();
    topics.topics().iter().map(|v| {
        v.name().to_owned()
    }).collect()
}