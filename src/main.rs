extern crate core;

use crate::dao::{Dao, DaoOps, Entry};
use crate::fetcher::{Fetcher, FetcherOps, GeoResponse};
use crate::scheduler::{Scheduler, SchedulerOps};
use crate::utils::{Wrapper, now_secs};
use clokwerk::Interval;
use config;
use futures::StreamExt;
use log::{error, info, warn};
use rusqlite::Connection;
use serde::{Deserialize, Deserializer};
use std::error::Error;
use std::fmt::Formatter;
use std::future::pending;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio_stream as stream;

mod dao;
mod fetcher;
mod scheduler;
mod utils;

#[derive(Deserialize)]
pub struct Conf {
    sub_groups: Vec<String>,
    geo_base_url: String,
    batch_size: usize,
    recheck_period: Wrapper<Interval>,
    update_period: Wrapper<Interval>,
}
async fn async_main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let conf: Conf = config::Config::builder()
        .add_source(config::File::with_name("conf.toml"))
        .build()?
        .try_deserialize()?;

    log4rs::init_file("log4rs.yml", Default::default())?;
    info!("Starting bot...");

    let conn = Connection::open("./db.db3")?;
    let dao = Arc::new(RwLock::new(Dao::new(conn)));
    let fetcher = Arc::new(Fetcher::new(conf.geo_base_url.clone()));

    let fetcher2 = fetcher.clone();
    let dao2 = dao.clone();
    tokio::task::spawn(async move {
        let mut scheduler = Scheduler::new(
            move || {
                let fetcher3 = fetcher2.clone();
                let dao3 = dao2.clone();
                let sub_groups = conf.sub_groups.clone();
                async move {
                    let par_stream = stream::iter(sub_groups)
                        .then(|group| {
                            let fetcher4 = fetcher3.clone();
                            async move {
                                match fetcher4.get_subs(group.clone()).await {
                                    Ok(value) => value,
                                    Err(err) => {
                                        error!(
                                            "Failed to get subscriptions from {}, error: {}",
                                            group, err
                                        );
                                        Vec::new()
                                    }
                                }
                            }
                        })
                        .flat_map(|v| stream::iter(v))
                        .map(|sub| {
                            let fetcher4 = fetcher3.clone();
                            async move {
                                match Fetcher::parse_link_to_ip(&sub) {
                                    Some(ip) => fetcher4
                                        .ping(ip)
                                        .await
                                        .inspect_err(|err| {
                                            error!("Unreachable sub: {}, err:{}", sub, err)
                                        })
                                        .ok()
                                        .map(|ping| (sub, ip, ping)),
                                    None => {
                                        warn!("Failed to parse sub: {}", sub);
                                        None
                                    }
                                }
                            }
                        })
                        .buffer_unordered(conf.batch_size)
                        .filter_map(async |e| e)
                        .chunks(conf.batch_size);

                    tokio::pin!(par_stream);

                    while let Some(batch) = par_stream.next().await {
                        match fetcher3
                            .get_geo(batch.iter().map(|(sub, ip, ping)| ip.to_string()).collect())
                            .await
                        {
                            Ok(res) => {
                                let now = now_secs();
                                let entries: Vec<Entry> = res
                                    .into_iter()
                                    .map(move |geo| {
                                        let (sub, io, ping) = batch
                                            .iter()
                                            .find(|(sub, ip, ping)| ip.to_string() == geo.query)
                                            .expect("Illegal state");
                                        Entry {
                                            url: sub.clone(),
                                            ping: ping.clone().cast_signed(),
                                            protocol: "".to_string(),
                                            country_code: geo.country_code,
                                            country: geo.country,
                                            city: geo.city,
                                            checked_at: now.cast_signed(),
                                        }
                                    })
                                    .collect();

                                match dao3.write_owned().await.insert_batch(entries) {
                                    Ok(val) => val,
                                    Err(err) => error!("Failed to insert batch: {}", err),
                                }
                            }
                            Err(err) => error!("Failed to get geo: {}", err),
                        }
                    }
                }
            },
            conf.update_period.0,
        );
        scheduler.start();

        pending::<()>().await;
    });
    // tokio::task::spawn(async move {
    //     let mut scheduler = Scheduler::new(|| {}, conf.recheck_period.0);
    //     scheduler.start();
    //     loop {}
    // });

    Ok(())
}

fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main())
}
