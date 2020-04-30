use rdkafka::*;
use rdkafka::config::RDKafkaLogLevel;

pub struct Config(pub(crate) ClientConfig);

impl Config {
    pub fn new() -> Config {
        Config(ClientConfig::new())
    }

    pub fn set<'a>(&'a mut self, key: &str, value: &str) -> &'a mut Config {
        let Config(client_config) = self;
        client_config.set(key, value);
        self
    }

    pub fn set_log_level(&mut self, log_level: RDKafkaLogLevel) -> &mut Config {
        let Config(client_config) = self;
        client_config.set_log_level(log_level);
        self
    }
}


