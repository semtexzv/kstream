use kstream::*;
use rdkafka::config::RDKafkaLogLevel;
use kstream::format::json::JSON;

use serde::{Serialize, Deserialize};
use kstream::store::{InMemory, Materialized};
use kstream::task::Task;

use kstream::stream::grouped::Count;

#[derive(Default, Debug, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize)]
struct User {
    name: String,
}


fn main() {
    std::env::set_var("RUST_LOG", "rdkafka=debug,trace");
    env_logger::init();
    let config = Config::new()
        .set("bootstrap.servers", "localhost:29092")
        .set("group.id", "base")
        .set("auto.offset.reset", "earliest")
        //  .set("debug", "all")
        .set_log_level(RDKafkaLogLevel::Debug);


    let task = Task::new(config.clone(), "basic")
        .stream::<JSON<String>, JSON<User>>("users");

    Task::new(config.clone(), "counts")
        .stream::<JSON<String>, JSON<User>>("users")
        .group()
        .aggregate::<Count, JSON<_>, JSON<_>, InMemory<_, _>>();

    task.map(|_, mut v| {
        v.name += " Processed";
        v
    }).table::<Materialized<JSON<_>, JSON<_>, InMemory<_, _>>>("users-state");


    //.to::<JSON<_>, JSON<_>>("users_processed");

    /*
    let mut users = Topic::<JSON<String>, JSON<User>>::named(&config, "users");
    users.send_next("Hello".to_string(), User { name: "World".to_string() });
    users.send_next("Hello".to_string(), User { name: "This".to_string() });
    users.send_next("Hello".to_string(), User { name: "Should".to_string() });
    users.send_next("Hello".to_string(), User { name: "Work".to_string() });
    users.send_next("Hello".to_string(), User { name: "World".to_string() });

    let c = config.clone();

    let users_ratings = Topic::<JSON<String>, JSON<User>>::named(&config.set_group("ratings"), "users_ratings");

    let c2 = c.clone();

    let a = std::thread::spawn(move || {
        let mut tbl: BoxedTable<String, User> = Box::new(
            users_ratings
                .map(|_k, v| {
                    println!("Received item in table stream");
                    v
                })
                .table::<Materialized<JSON<_>, JSON<_>, InMemory<_, _>>>(c2, "user_ratings".to_string()));
        tbl.poll();
    });


    let b = std::thread::spawn(move || {
        users
            .map(|k: &String, v: User| {
                println!("MSG {:?} -> {:?}", k, v);
                return v;
            })
            .sink_to(Topic::<JSON<String>, JSON<User>>::named(&c, "users_ratings"));
    });

    println!("Waiting");
    a.join().unwrap();
    b.join().unwrap();
     */
}