#[macro_use]
extern crate log;

use rdkafka::admin::AdminOptions;

mod common;

use common::*;

use kstream::stream::topic::{TypedProducer, RawProducer};
use kstream::stream::KSink;
use kstream::format::json::JSON;

use serde::{Serialize, Deserialize};
use kstream::task::Task;
use std::time::Duration;

use futures::future::FutureExt;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct User {
    name: String,
    email: String,
    verified: bool,
}

#[tokio::test]
async fn test_topics() {
    init();
    clear_topics().await;
    let cfg = cfg();

    let mut producer = TypedProducer::<JSON<i32>, JSON<_>>::from(RawProducer::new(&cfg, "users"));

    for i in 0..10 {
        producer.send_next(None, &0, Some(&User {
            name: "Abott".to_string(),
            email: "frankie@gmail.com".to_string(),
            verified: false,
        })).await;

        producer.send_next(None, &1, Some(&User {
            name: "Costello".to_string(),
            email: "invalid(at)gmail.com".to_string(),
            verified: false,
        })).await;
    }
    assert!(get_topics().contains(&"users".to_string()));

    // Simplest processing task, read from users,
    // Calculate whether the email is valid and store into users_valid topic
    let t1 = Task::new(cfg.clone(), "verifier")
        .stream::<JSON<usize>, JSON<User>>("users")
        .map(|k, v| {
            info!("Checking email validity for {:?}", k);
            if v.email.contains("@") {
                return true;
            }
            return false;
        })
        .to::<JSON<_>, JSON<bool>>("users_status");


    let t2 = Task::new(cfg, "invalid_detector")
        .stream::<JSON<usize>, JSON<bool>>("users_status")
        .filter(|k, v| {
            info!("Filtering by status {:?}", k);
            return *v;
        })
        .to::<JSON<_>, JSON<bool>>("users_valid");

    let t3 = tokio::time::timeout(Duration::from_secs(15), futures::future::pending::<()>());
    let _res = tokio::select! {
        _ = t1.fuse() => (),
        _ = t2.fuse() => (),
        _ = t3.fuse() => (),
    };
    assert!(get_topics().contains(&"users_valid".to_string()));
    info!("OK")
}

/*
#[tokio::test]
async fn test_agg() {
    //init();
    clear_topics().await;
    let cfg = cfg();
}

 */