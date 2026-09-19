use crate::utils::now_secs;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio_rusqlite::fallible_iterator::{FallibleIterator, IteratorExt};
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
        row.get::<usize, i64>(2),
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
    pub ping: i64,
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
    pub ping: i64,
    pub protocol: String,
    pub country_code: String,
    pub country: String,
    pub city: String,
    pub checked_at: i64,
}

impl Into<Entry> for ExistedEntry {
    fn into(self) -> Entry {
        Entry {
            url: self.url,
            ping: self.ping,
            protocol: self.protocol,
            country_code: self.country_code,
            country: self.country,
            city: self.city,
            checked_at: self.checked_at,
        }
    }
}

pub trait DaoOps {
    async fn init(&self) -> Result<()>;
    async fn delete(&self, ids: Vec<Id>) -> Result<()>;
    async fn insert_batch(&self, batch: Vec<Entry>) -> Result<()>;
    async fn list(&self, limit: u32, page: Option<u32>) -> Result<Vec<ExistedEntry>>;
    async fn list_by_country_code(
        &self,
        country_code: String,
        limit: u32,
        page: Option<u32>,
    ) -> Result<Vec<ExistedEntry>>;
    async fn list_by_country_code_and_city(
        &self,
        country_code: String,
        city: String,
        limit: u32,
        page: Option<u32>,
    ) -> Result<Vec<ExistedEntry>>;

    async fn get_county_codes_to_cities(&self) -> Result<HashMap<String, HashSet<String>>>;

    async fn list_except_last_period(&self, period: Duration) -> Result<Vec<ExistedEntry>>;

    async fn update(&self, entries: Vec<ExistedEntry>) -> Result<()>;
}

impl DaoOps for Dao {
    async fn init(&self) -> Result<()> {
        self.connection.call(|c| c.execute(
            "CREATE TABLE IF NOT EXISTS entries (id INTEGER PRIMARY KEY, url VARCHAR, ping INTEGER, protocol VARCHAR, country_code VARCHAR, country VARCHAR, city VARCHAR, checked_at INTEGER)",
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
        let num_params = 7;
        let placeholders = std::iter::repeat("(NULL, ?, ?, ?, ?, ?, ?, ?)")
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
                        acc.and_then(|_| statement.raw_bind_parameter(i * num_params + 1, entry.url))
                        .and_then(|_| statement.raw_bind_parameter(i * num_params + 2, entry.ping))
                        .and_then(|_| statement.raw_bind_parameter(i * num_params + 3, entry.protocol))
                        .and_then(|_| {
                            statement.raw_bind_parameter(i * num_params + 4, entry.country_code)
                        })
                        .and_then(|_| statement.raw_bind_parameter(i * num_params + 5, entry.country))
                        .and_then(|_| statement.raw_bind_parameter(i * num_params + 6, entry.city))
                        .and_then(|_| {
                            statement.raw_bind_parameter(i * num_params + 7, entry.checked_at)
                        })
                })?;

            statement.raw_execute()
                }).await?;
        Ok(())
    }

    async fn list(&self, limit: u32, page: Option<u32>) -> Result<Vec<ExistedEntry>> {
        match page {
            Some(p) => {
                self.connection
                    .call(move |c| {
                        let mut s = c.prepare(
                            format!(
                                "SELECT {} ORDER BY e.ping ASC LIMIT ?1 OFFSET ?2",
                                Self::SELECT_CLAUSE
                            )
                            .as_str(),
                        )?;
                        s.query_map(params![limit, p], Self::MAPPER)?.collect()
                    })
                    .await
            }
            None => {
                self.connection
                    .call(move |c| {
                        let mut s = c.prepare(
                            format!(
                                "SELECT {} ORDER BY e.ping ASC LIMIT ?1",
                                Self::SELECT_CLAUSE
                            )
                            .as_str(),
                        )?;
                        s.query_map(params![limit], Self::MAPPER)?.collect()
                    })
                    .await
            }
        }
    }

    async fn list_by_country_code(
        &self,
        country_code: String,
        limit: u32,
        page: Option<u32>,
    ) -> Result<Vec<ExistedEntry>> {
        match page {
            Some(o) => {
                 self.connection.call(move |c|{
                     let mut s =c.prepare(format!("SELECT {} WHERE LOWER(e.country_code) = LOWER(?1) ORDER BY e.ping ASC LIMIT ?2 OFFSET ?3", Self::SELECT_CLAUSE).as_str())?;
                s.query_map(params![country_code, limit, o], Self::MAPPER)?
                    .collect()
            }).await
            }
            None => {
                self.connection.call(move |c|{
                    let mut s = c.prepare(
                        format!(
                            "SELECT {} WHERE LOWER(e.country_code) = LOWER(?1) ORDER BY e.ping ASC LIMIT ?2",
                            Self::SELECT_CLAUSE
                        )
                            .as_str(),
                    )?;
                    s.query_map(params![country_code, limit], Self::MAPPER)?.collect()
                }).await
            }
        }
    }

    async fn list_by_country_code_and_city(
        &self,
        country_code: String,
        city: String,
        limit: u32,
        page: Option<u32>,
    ) -> Result<Vec<ExistedEntry>> {
        match page {
            Some(o) => {
                self.connection.call(move |c|{
                    let mut s =c.prepare(format!("SELECT {} WHERE LOWER(e.country_code) = LOWER(?1) AND LOWER(e.city) = LOWER(?2) ORDER BY e.ping ASC LIMIT ?3 OFFSET ?4", Self::SELECT_CLAUSE).as_str())?;
                    s.query_map(params![country_code, city, limit, o], Self::MAPPER)?
                        .collect()
                }).await
            }
           None => {
                self.connection.call(move |c|{
                    let mut s = c.prepare(
                        format!(
                            "SELECT {} WHERE LOWER(e.country_code) = LOWER(?1) AND LOWER(e.city) = LOWER(?2) ORDER BY e.ping ASC LIMIT ?3",
                            Self::SELECT_CLAUSE
                        )
                            .as_str(),
                    )?;
                    s.query_map(params![country_code, city, limit], Self::MAPPER)?.collect()
                }).await
            }
        }
    }

    async fn get_county_codes_to_cities(&self) -> Result<HashMap<String, HashSet<String>>> {
        self.connection
            .call(|c| {
                let mut res = HashMap::new();
                let mut s = c.prepare("SELECT DISTINCT e.country_code, e.city FROM entries e")?;
                s.query_map((), |row| {
                    match (row.get::<usize, String>(0), row.get::<usize, String>(1)) {
                        (Ok(cc), Ok(city)) => Ok((cc, city)),
                        _ => Err(Error::QueryReturnedNoRows),
                    }
                })?
                .filter_map(|e| e.ok())
                .for_each(|(cc, city)| {
                    res.entry(cc).or_insert_with(HashSet::new).insert(city);
                });
                Ok(res)
            })
            .await
    }

    async fn list_except_last_period(&self, period: Duration) -> Result<Vec<ExistedEntry>> {
        let less_that = (now_secs() - period.as_secs()).cast_signed();
        self.connection
            .call(move |c| {
                let mut s = c.prepare(
                    format!(
                        "SELECT {} WHERE e.checked_at <= ?1 ORDER BY e.ping DESC",
                        Self::SELECT_CLAUSE
                    )
                    .as_str(),
                )?;
                s.query_map(params![less_that], Self::MAPPER)?.collect()
            })
            .await
    }

    async fn update(&self, entries: Vec<ExistedEntry>) -> Result<()> {
        self.connection
            .call(move |c| {
                for e in entries {
                    let mut s = c.prepare("UPDATE entries SET url=?1, ping=?2, country_code=?3, country=?4, city=?5, protocol=?6, checked_at=?7 WHERE id = ?8")?;
                    s.execute(params![e.url, e.ping, e.country_code, e.country, e.city, e.protocol, e.checked_at, e.id])?;
                }
                Ok(())
            })
            .await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use {prop::prelude::*, proptest as prop};

    async fn init() -> Dao {
        let conn: Connection = Connection::open_in_memory().await.unwrap();
        let mut dao = Dao::new(conn);
        dao.init().await.unwrap();
        dao
    }

    fn entry() -> impl Strategy<Value = Entry> {
        let url = prop::sample::select([
            "hy2://1beb216e3f60aea9555dae60d219a5ca@152-69-231-175.liao.kdns.fr:50160/?sni=152-69-231-175.liao.kdns.fr#咕",
            "hy2://14f7504c-bd01-4d88-bd42-80dc6bf6b202@samurai.h4ck.me:443/#芬兰",
            "vmess://eyJ2IjoiMiIsInBzIjoiXHVkODNjXHVkZGZhXHVkODNjXHVkZGY4IEpvaW4rVGVsZWdyYW06QEZhcmFoX1ZQTiBcdWQ4M2NcdWRkZmFcdWQ4M2NcdWRkZjgiLCJhZGQiOiI4Mi4xOTguMjQ2LjIzMyIsInBvcnQiOiIxODAiLCJ0eXBlIjoibm9uZSIsImlkIjoiZDEzZmMyZjUtM2UwNS00Nzk1LTgxZWItNDQxNDNhMDllNTUyIiwiYWlkIjoiMCIsIm5ldCI6InRjcCIsInBhdGgiOiIvIiwiaG9zdCI6IiIsInRscyI6IiIsInNraXAtY2VydC12ZXJpZnkiOnRydWV9",
            "vless://a8e3155b-ceb1-4fcb-bc0c-2e77ec005401@api.noneok.com:443?mode=gun&security=reality&encryption=none&authority=v2rayNplus--v2rayNplus--v2rayNplus&pbk=S8O8R938N960cpQfIIDXsJTxeGAkbVv6PlIqP0-d30w&type=grpc&serviceName=api.v1.StreamService&sni=api.noneok.com&sid=1ea59febb8d4fc8e#%D8%A7%DA%AF%D9%87%20%D9%85%DB%8C%D8%AE%D9%88%D8%A7%DB%8C%20%D9%82%D8%B7%D8%B9%20%D9%86%D8%B4%DB%8C%20%D8%AC%D9%88%DB%8C%D9%86%20%D8%B4%D9%88%20%3A%20%40farsiproxy",
            "vless://8c561eb2-f643-49ce-b5b6-81690ec268c0@b2n.ir:2087?mode=auto&path=%2FFiShChIpS&security=tls&encryption=none&extra=%7B%22mode%22%3A%22auto%22%2C%22xPaddingBytes%22%3A%221-1%22%2C%22xPaddingObfsMode%22%3Atrue%2C%22xPaddingKey%22%3A%22ctx%22%2C%22xPaddingHeader%22%3A%22x-grpc-context%22%2C%22xPaddingMethod%22%3A%22tokenish%22%2C%22sessionIDPlacement%22%3A%22header%22%2C%22sessionIDKey%22%3A%22Idempotency-Key%22%2C%22seqPlacement%22%3A%22header%22%2C%22seqKey%22%3A%22Upload-Offset%22%2C%22sessionPlacement%22%3A%22header%22%2C%22sessionKey%22%3A%22Idempotency-Key%22%7D&insecure=0&host=fish.kaftarkakolbesarwifi.ir&fp=chrome&type=xhttp&allowInsecure=0&sni=fish.kaftarkakolbesarwifi.ir#%D8%A7%DA%AF%D9%87%20%D9%85%DB%8C%D8%AE%D9%88%D8%A7%DB%8C%20%D9%82%D8%B7%D8%B9%20%D9%86%D8%B4%DB%8C%20%D8%AC%D9%88%DB%8C%D9%86%20%D8%B4%D9%88%20%3A%20%40farsiproxy",
        ].as_slice());
        let country = prop::sample::select(["USA", "Canada", "Mexico"].as_slice());
        let city = prop::sample::select(["New York", "Toronto", "Mexico City"].as_slice());
        let country_code = prop::sample::select(["US", "EN", "FR"].as_slice());
        let now = now_secs().cast_signed();

        (url, country, city, country_code).prop_map(move |(url, country, city, country_code)| {
            Entry {
                url: url.to_string(),
                country: country.to_string(),
                city: city.to_string(),
                ping: 0,
                protocol: "".to_string(),
                country_code: country_code.to_string(),
                checked_at: now,
            }
        })
    }

    fn vec_gen<T: Strategy>(value: T) -> impl Strategy<Value = Vec<T::Value>> {
        prop::collection::vec(value, 10)
    }

    proptest! {
        #[test]
        fn insert_and_list(entries in vec_gen(entry())) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let dao = init().await;
                dao.insert_batch(entries.clone()).await.unwrap();
                let res_entries = dao.list_except_last_period(Duration::ZERO).await.unwrap();
                assert_eq!(res_entries.len(), entries.len());
            })
        }

        #[test]
        fn insert_and_list_cc_and_city(entries in vec_gen(entry())) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let dao = init().await;
                dao.insert_batch(entries.clone()).await.unwrap();
                let res_entries = dao.get_county_codes_to_cities().await.unwrap();
                let mut cc_cities  = HashMap::new();
                entries.into_iter().for_each(|e|{
                    cc_cities.entry(e.country_code).or_insert_with(HashSet::new).insert(e.city);
                });

                assert_eq!(res_entries, cc_cities);
            })
        }

        #[test]
        fn insert_and_delete(entries in vec_gen(entry())) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let dao = init().await;
                dao.insert_batch(entries.clone()).await.unwrap();
                let inserted:HashSet<Id> = dao.list_except_last_period(Duration::ZERO).await.unwrap().into_iter().map(|e|e.id).collect();
                let to_del: HashSet<Id> = inserted.clone().into_iter().skip(2).take(4).collect();
                dao.delete(to_del.clone().into_iter().collect()).await.unwrap();
                let res: HashSet<Id> = dao.list_except_last_period(Duration::ZERO).await.unwrap().iter().map(|e|e.id).collect();

                assert_eq!(res, inserted.difference(&to_del).cloned().collect());
            })
        }

        #[test]
        fn insert_and_update(entries in vec_gen(entry())) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let dao = init().await;
                dao.insert_batch(entries.clone()).await.unwrap();
                let expected: Vec<ExistedEntry> = dao.list(10, None).await.unwrap().into_iter().map(|ee| ExistedEntry{
                    city: "some_city".to_string(),
                    country: "some_country".to_string(),
                    ping: 10,
                    country_code: "cc".to_string(),
                    ..ee
                }).collect();
                dao.update(expected.clone()).await.unwrap();
                let actual = dao.list(10, None).await.unwrap();

                assert_eq!(expected, actual);
            })
        }
    }
}
