use crate::utils::now_secs;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_rusqlite::rusqlite::Error;
use tokio_rusqlite::{Connection, Result, Row, params, params_from_iter};

pub struct Dao {
    connection: Connection,
}

unsafe impl Send for Dao {}

impl Dao {
    pub fn new(connection: Connection) -> Self {
        Dao { connection }
    }

    const SELECT_CLAUSE: &str = "e.id, e.url, e.ping, e.protocol, e.country_code, e.country, e.city, e.checked_at FROM entries e";

    const MAPPER: fn(&Row) -> std::result::Result<ExistedEntry, Error> = |row: &Row| match (
        row.get::<usize, Id>(0),
        row.get::<usize, String>(1),
        row.get::<usize, i32>(2),
        row.get::<usize, String>(3),
        row.get::<usize, String>(4),
        row.get::<usize, String>(5),
        row.get::<usize, String>(6),
        row.get::<usize, i64>(7),
    ) {
        (
            Ok(id),
            Ok(url),
            Ok(ping),
            Ok(protocol),
            Ok(country_code),
            Ok(country),
            Ok(city),
            Ok(checked_at),
        ) => Ok(ExistedEntry {
            id,
            url,
            ping,
            protocol,
            country_code,
            country,
            city,
            checked_at,
        }),
        _ => Err(Error::QueryReturnedNoRows),
    };
}

pub type Id = i64;

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize, Eq, Ord)]
pub struct Entry {
    pub url: String,
    pub ping: i32,
    pub protocol: String,
    pub country_code: String,
    pub country: String,
    pub city: String,
    pub checked_at: i64,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize, Eq, Ord)]
pub struct ExistedEntry {
    pub id: Id,
    pub url: String,
    pub ping: i32,
    pub protocol: String,
    pub country_code: String,
    pub country: String,
    pub city: String,
    pub checked_at: i64,
}

pub trait DaoOps {
    async fn init(&self) -> Result<()>;
    async fn delete(&self, ids: Vec<Id>) -> Result<()>;
    async fn insert_batch(&self, batch: Vec<Entry>) -> Result<()>;
    async fn list_by_country_code(
        &self,
        country_code: String,
        limit: Option<u32>,
        page: Option<u32>,
    ) -> Result<Vec<ExistedEntry>>;

    async fn list_except_last_period(&self, period: Duration) -> Result<Vec<ExistedEntry>>;
}

impl DaoOps for Dao {
    async fn init(&self) -> Result<()> {
        self.connection.call(|c| c.execute(
            "CREATE TABLE IF NOT EXISTS entries (id INTEGER PRIMARY KEY, url, ping INTEGER, protocol VARCHAR, country_code VARCHAR, country VARCHAR, city VARCHAR)",
            (),
        )).await?;
        Ok(())
    }

    async fn delete(&self, ids: Vec<Id>) -> Result<()> {
        let placeholders = std::iter::repeat("?")
            .take(ids.len())
            .collect::<Vec<_>>()
            .join(",");

        self.connection
            .call(move |c| {
                let mut statement = c.prepare(&format!(
                    "DELETE FROM entries WHERE id IN ({})",
                    placeholders
                ))?;
                statement.execute(params_from_iter(ids))
            })
            .await?;
        Ok(())
    }

    async fn insert_batch(&self, batch: Vec<Entry>) -> Result<()> {
        let num_params = 6;
        let placeholders = std::iter::repeat("(?, ?, ?, ?, ?, ?)")
            .take(batch.len())
            .collect::<Vec<_>>()
            .join(",");

        self.connection.call(move |c| {
            let mut statement = c.prepare(&format!(
                "INSERT INTO entries(id, url, ping, protocol, country_code, country, city, checked_at) VALUES {}",
                placeholders
            ))?;

            batch
                .into_iter()
                .enumerate()
                .fold(Ok(()), |acc, (i, entry)| {
                    acc.and_then(|_| statement.raw_bind_parameter(i * num_params + 1, "NULL"))
                        .and_then(|_| statement.raw_bind_parameter(i * num_params + 2, entry.url))
                        .and_then(|_| statement.raw_bind_parameter(i * num_params + 3, entry.ping))
                        .and_then(|_| statement.raw_bind_parameter(i * num_params + 4, entry.protocol))
                        .and_then(|_| {
                            statement.raw_bind_parameter(i * num_params + 5, entry.country_code)
                        })
                        .and_then(|_| statement.raw_bind_parameter(i * num_params + 6, entry.country))
                        .and_then(|_| statement.raw_bind_parameter(i * num_params + 7, entry.city))
                        .and_then(|_| {
                            statement.raw_bind_parameter(i * num_params + 8, entry.checked_at)
                        })
                })?;

            statement.raw_execute()
                }).await?;
        Ok(())
    }

    async fn list_by_country_code(
        &self,
        country_code: String,
        limit: Option<u32>,
        page: Option<u32>,
    ) -> Result<Vec<ExistedEntry>> {
        match (limit, page) {
            (Some(l), Some(o)) => {
                 self.connection.call(move |c|{
                     let mut s =c.prepare(format!("SELECT {} WHERE LOWER(e.country_code) = LOWER(?1) ORDER BY e.ping ASC LIMIT ?2 OFFSET ?3", Self::SELECT_CLAUSE).as_str())?;
                s.query_map(params![country_code, l, o], Self::MAPPER)?
                    .collect()
            }).await
            }
            _ => {
                self.connection.call(move |c|{
                    let mut s = c.prepare(
                        format!(
                            "SELECT {} WHERE LOWER(e.country_code) = LOWER(?1) ORDER BY e.ping ASC",
                            Self::SELECT_CLAUSE
                        )
                            .as_str(),
                    )?;
                    s.query_map(params![country_code], Self::MAPPER)?.collect()
                }).await
            }
        }
    }

    async fn list_except_last_period(&self, period: Duration) -> Result<Vec<ExistedEntry>> {
        let less_that = (now_secs() - period.as_secs()).cast_signed();
        self.connection
            .call(move |c| {
                let mut s = c.prepare(
                    format!(
                        "SELECT {} WHERE e.checked_at < ?1 ORDER BY e.ping DESC",
                        Self::SELECT_CLAUSE
                    )
                    .as_str(),
                )?;
                s.query_map(params![less_that], Self::MAPPER)?.collect()
            })
            .await
    }
}
