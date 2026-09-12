extern crate core;

use clokwerk::Interval;
use config;
use log::info;
use rusqlite::Connection;
use serde::{Deserialize, Deserializer};
use std::error::Error;
use std::fmt::Formatter;
use tokio::runtime;
use crate::utils::Wrapper;

mod scheduler;
mod utils;

#[derive(Deserialize)]
pub struct Conf {
    subs: Vec<String>,
    geo_base_url: String,
    default_recheck_period: Wrapper<Interval>,
}
async fn async_main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let conf: Conf = config::Config::builder()
        .add_source(config::File::with_name("conf.toml"))
        .build()?
        .try_deserialize()?;

    log4rs::init_file("log4rs.yml", Default::default())?;
    info!("Starting bot...");

    let conn = Connection::open("./db.db3")?;

    Ok(())
}

fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main())
}
