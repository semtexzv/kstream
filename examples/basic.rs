use kstream::*;
use rdkafka::config::RDKafkaLogLevel;
use kstream::format::json::JSON;
use kstream::stream::Topic;
use kstream::stream::join::JoinType;

#[derive(Default, Debug)]
struct User {
    name: String,
}


fn main() {
    let mut config = Config::new();
    config
        .set("group.id", "groupid")
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set_log_level(RDKafkaLogLevel::Debug);

    let mut state = 0;

    let users = Topic::<JSON<String>, JSON<User>>::named(&config, "users");
    let users_ratings = Topic::<JSON<String>, JSON<User>>::named(&config, "users_ratings");

    users.join(JoinType::Inner, users_ratings)
        .map(|k: String, v: (User, User)| {
            return (k.len(), v.name);
        })
        .sink_to(Topic::<JSON<_>, JSON<_>>::named(&config, "whatever_len"));


    println!("Customer topic:");
}