extern crate core;

use crate::api::Api;
use crate::dao::{Dao, DaoOps, Entry, ExistedEntry, Id};
use crate::fetcher::{Fetcher, FetcherOps, GeoResponse};
use crate::scheduler::{Scheduler, SchedulerOps};
use crate::utils::{Wrapper, now_secs};
use axum::Router;
use axum::extract::Query;
use axum::routing::get;
use clokwerk::Interval;
use config;
use futures::{FutureExt, StreamExt, TryFutureExt};
use log::{error, info, warn};
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
use tokio_rusqlite::Connection;
use tokio_stream as stream;

mod api;
mod dao;
mod fetcher;
mod scheduler;
mod utils;

#[derive(Deserialize)]
pub struct Conf {
    sub_groups: Vec<String>,
    geo_base_url: String,
    batch_size: usize,
    port: u16,
    recheck_period: Wrapper<Duration>,
    update_period: Wrapper<Duration>,
}

async fn async_main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let conf: Conf = config::Config::builder()
        .add_source(config::File::with_name("conf.toml"))
        .build()?
        .try_deserialize()?;

    log4rs::init_file("log4rs.yml", Default::default())?;
    info!("Starting app...");

    let conn = Connection::open("./db.db3").await?;
    let dao = Arc::new(Dao::new(conn));
    let fetcher = Arc::new(Fetcher::new(conf.geo_base_url.clone()));

    let router = Router::new()
        .route("/subs", get(Api::get_subs))
        .route("/dict", get(Api::get_cc_to_city))
        .with_state(Api::new(dao.clone()));

    dao.init().await?;

    let fetcher2 = fetcher.clone();
    let dao2 = dao.clone();
    let sub_groups = conf.sub_groups.clone();
    let mut update_scheduler = Scheduler::new(
        move || {
            let fetcher3 = fetcher2.clone();
            let dao3 = dao2.clone();
            let sub_groups = sub_groups.clone();
            async move {
                let par_stream = stream::iter(sub_groups)
                    .then(|group| {
                        let fetcher4 = fetcher3.clone();
                        async move {
                            match fetcher4.get_subs(group.clone()).await {
                                Ok(value) => {
                                    info!("Got sub group: {}, sub count: {}", group, value.len());
                                    value
                                }
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
                        match Fetcher::parse_link_to_ip(&sub) {
                            Some(ip) => {
                                info!("Pinging ip: {}", ip);
                                async move {
                                    fetcher4.ping(ip).map(move |ping| match ping {
                                        Ok(p) => {
                                            info!("Ping result: {}ms", p);
                                            Some((sub, ip, p))
                                        }
                                        Err(err) => {
                                            error!("Unreachable sub: {}, err: {}", sub, err);
                                            None
                                        }
                                    })
                                }
                                .flatten()
                                .boxed()
                            }
                            None => {
                                warn!("Failed to parse sub: {}", sub);
                                futures::future::ready(None).boxed()
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
                                        ping: ping.clone(),
                                        protocol: "".to_string(),
                                        country_code: geo.country_code,
                                        country: geo.country,
                                        city: geo.city,
                                        checked_at: now.cast_signed(),
                                    }
                                })
                                .collect();

                            info!("Inserting batch");
                            dao3.insert_batch(entries)
                                .await
                                .unwrap_or_else(|err| error!("Failed to insert batch: {}", err))
                        }
                        Err(err) => error!("Failed to get geo: {}", err),
                    }
                }
            }
        },
        conf.update_period.into(),
    );

    let dao2 = dao.clone();
    let fetcher2 = fetcher.clone();
    let recheck_period = conf.recheck_period.0;
    let mut recheck_scheduler = Scheduler::new(
        move || {
            let dao2 = dao2.clone();
            let fetcher2 = fetcher2.clone();
            let recheck_period = recheck_period.clone();
            async move {
                match dao2.list_except_last_period(recheck_period).await {
                    Ok(entries) => {
                        let res: Vec<Result<ExistedEntry, Id>> = stream::iter(entries)
                            .map(|ee| {
                                fetcher2
                                    .ping(Fetcher::parse_link_to_ip(&ee.url).unwrap())
                                    .map(move |ping| match ping {
                                        Ok(p) => {
                                            info!("Recheck ping result: {}ms", p);
                                            Ok(ExistedEntry {
                                                ping: p,
                                                checked_at: now_secs().cast_signed(),
                                                ..ee
                                            })
                                        }
                                        Err(err) => {
                                            error!("Recheck failed. Sub: {}, err: {}", ee.url, err);
                                            Err(ee.id)
                                        }
                                    })
                            })
                            .buffer_unordered(conf.batch_size)
                            .collect()
                            .await;

                        let (to_update, to_delete): (Vec<_>, Vec<_>) =
                            res.into_iter().partition(|el| el.is_ok());

                        dao2.delete(to_delete.into_iter().flat_map(|el| el.err()).collect())
                            .await
                            .inspect_err(|err| error!("Failed to delete: {}", err));
                        dao2.update(to_update.into_iter().flat_map(|el| el.ok()).collect())
                            .await
                            .inspect_err(|err| error!("Failed to update: {}", err));
                    }
                    Err(err) => {
                        error!("Failed to list entries for recheck. Err: {}", err)
                    }
                }
            }
        },
        Wrapper(conf.recheck_period.0).into(),
    );

    update_scheduler.start();
    recheck_scheduler.start();
    tokio::spawn(update_scheduler.run_task());

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", conf.port)).await?;
    axum::serve(listener, router).await?;

    Ok(())
}

fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main())
}
