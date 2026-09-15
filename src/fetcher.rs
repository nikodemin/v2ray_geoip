use ping as ping_mod;
use regex::Regex;
use reqwest::{Client, Error};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;

pub struct Fetcher {
    geo_base_ip: String,
    client: Client,
}

impl Fetcher {
    pub fn new(geo_base_ip: String) -> Self {
        Fetcher {
            geo_base_ip,
            client: Client::new(),
        }
    }

    pub fn parse_link_to_ip(link: &String) -> Option<IpAddr> {
        let regex = Regex::new(r"@([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})").unwrap();

        regex
            .captures(link.as_str())
            .iter()
            .flat_map(|captures| captures.get(1).map(|m| m.as_str()))
            .next()
            .iter()
            .flat_map(|s| IpAddr::from_str(s))
            .next()
    }
}

#[derive(Serialize)]
struct GeoRequest {
    pub query: String,
    pub fields: String,
    pub lang: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GeoResponse {
    pub country_code: String,
    pub country: String,
    pub city: String,
    pub query: String,
}

pub trait FetcherOps {
    async fn get_subs(&self, url: String) -> Result<Vec<String>, Error>;
    async fn ping(&self, ip: IpAddr) -> Result<u32, ping_mod::Error>;
    async fn get_geo(&self, ips: Vec<String>) -> Result<Vec<GeoResponse>, Error>;
}

impl FetcherOps for Fetcher {
    async fn get_subs(&self, url: String) -> Result<Vec<String>, Error> {
        let resp = self
            .client
            .get(url)
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .text()
            .await?;

        Ok(resp
            .lines()
            .into_iter()
            .skip_while(|s| s.starts_with("#") || *s == "\n")
            .map(|s| s.to_string())
            .collect())
    }

    async fn ping(&self, ip: IpAddr) -> Result<u32, ping_mod::Error> {
        ping_mod::new(ip)
            .timeout(Duration::from_secs(5))
            .send()
            .map(|r| r.rtt.as_millis() as u32)
    }

    async fn get_geo(&self, ips: Vec<String>) -> Result<Vec<GeoResponse>, Error> {
        let req: Vec<GeoRequest> = ips
            .into_iter()
            .map(|ip| GeoRequest {
                query: ip,
                fields: "city,country,countryCode,query".to_string(),
                lang: "en".to_string(),
            })
            .collect();

        self.client
            .post(self.geo_base_ip.clone() + "/batch")
            .json(&req)
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .json::<Vec<GeoResponse>>()
            .await
    }
}
