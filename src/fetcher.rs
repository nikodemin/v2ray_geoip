use base64::Engine;
use base64::engine::general_purpose;
use dns_lookup::lookup_host;
use futures::FutureExt;
use ping as ping_mod;
use regex::Regex;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Error};
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;

pub struct Fetcher {
    geo_base_ip: String,
    client: ClientWithMiddleware,
}

impl Fetcher {
    pub fn new(geo_base_ip: String, retries: u32) -> Self {
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(retries);

        let client: ClientWithMiddleware = ClientBuilder::new(reqwest::Client::new())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        Fetcher {
            geo_base_ip,
            client,
        }
    }

    pub fn parse_link_to_ip(link: &String) -> Option<IpAddr> {
        let host_regex = Regex::new(r"@(.+:\d+)").unwrap();
        let suffix_regex = Regex::new(r":\d+").unwrap();
        let protocol_regex = Regex::new(r"^.+://").unwrap();

        let parse = |link: &str| -> Option<IpAddr> {
            host_regex
                .captures(link)
                .iter()
                .flat_map(|captures| captures.get(1).map(|m| m.as_str()))
                .next()
                .iter()
                .flat_map(|s| {
                    IpAddr::from_str(s).ok().or_else(|| {
                        let host = suffix_regex.replace(s, "");
                        lookup_host(host.as_ref())
                            .into_iter()
                            .flat_map(|mut e| e.next())
                            .next()
                    })
                })
                .next()
        };

        if link.contains("@") {
            parse(link)
        } else {
            let bytes = general_purpose::STANDARD
                .decode(protocol_regex.replace(link, "").as_ref())
                .ok()?;
            let json_str = String::from_utf8(bytes).ok()?;
            let json: Value = serde_json::from_str(json_str.as_str()).ok()?;
            json["add"]
                .as_str()
                .iter()
                .flat_map(|s| IpAddr::from_str(s).ok())
                .next()
        }
    }

    pub fn parse_protocol(link: &String) -> Option<String> {
        let protocol_regex = Regex::new(r"^(.+)://").unwrap();

        protocol_regex
            .captures(link)
            .iter()
            .flat_map(|captures| captures.get(1).map(|m| m.as_str().to_string()))
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
    async fn ping(&self, ip: IpAddr) -> Result<i64, ping_mod::Error>;
    async fn get_geo(
        &self,
        ips: Vec<String>,
    ) -> Result<Vec<GeoResponse>, Box<dyn std::error::Error + Send + Sync>>;
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

    fn ping(&self, ip: IpAddr) -> impl use<> + Future<Output = Result<i64, ping_mod::Error>> {
        async move {
            ping_mod::new(ip)
                .timeout(Duration::from_secs(5))
                .send_async()
                .await
                .map(|r| r.rtt.as_millis() as i64)
        }
    }

    async fn get_geo(
        &self,
        ips: Vec<String>,
    ) -> Result<Vec<GeoResponse>, Box<dyn std::error::Error + Send + Sync>> {
        let req: Vec<GeoRequest> = ips
            .into_iter()
            .map(|ip| GeoRequest {
                query: ip,
                fields: "city,country,countryCode,query".to_string(),
                lang: "en".to_string(),
            })
            .collect();

        let res = self
            .client
            .post(self.geo_base_ip.clone() + "/batch")
            .json(&req)
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .json::<Vec<GeoResponse>>()
            .await?;

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_link_to_ip() {
        let links = vec![
            "hy2://1beb216e3f60aea9555dae60d219a5ca@152-69-231-175.liao.kdns.fr:50160/?sni=152-69-231-175.liao.kdns.fr#咕",
            "hy2://14f7504c-bd01-4d88-bd42-80dc6bf6b202@samurai.h4ck.me:443/#芬兰",
            "vmess://eyJ2IjoiMiIsInBzIjoiXHVkODNjXHVkZGZhXHVkODNjXHVkZGY4IEpvaW4rVGVsZWdyYW06QEZhcmFoX1ZQTiBcdWQ4M2NcdWRkZmFcdWQ4M2NcdWRkZjgiLCJhZGQiOiI4Mi4xOTguMjQ2LjIzMyIsInBvcnQiOiIxODAiLCJ0eXBlIjoibm9uZSIsImlkIjoiZDEzZmMyZjUtM2UwNS00Nzk1LTgxZWItNDQxNDNhMDllNTUyIiwiYWlkIjoiMCIsIm5ldCI6InRjcCIsInBhdGgiOiIvIiwiaG9zdCI6IiIsInRscyI6IiIsInNraXAtY2VydC12ZXJpZnkiOnRydWV9",
            "vless://a8e3155b-ceb1-4fcb-bc0c-2e77ec005401@api.noneok.com:443?mode=gun&security=reality&encryption=none&authority=v2rayNplus--v2rayNplus--v2rayNplus&pbk=S8O8R938N960cpQfIIDXsJTxeGAkbVv6PlIqP0-d30w&type=grpc&serviceName=api.v1.StreamService&sni=api.noneok.com&sid=1ea59febb8d4fc8e#%D8%A7%DA%AF%D9%87%20%D9%85%DB%8C%D8%AE%D9%88%D8%A7%DB%8C%20%D9%82%D8%B7%D8%B9%20%D9%86%D8%B4%DB%8C%20%D8%AC%D9%88%DB%8C%D9%86%20%D8%B4%D9%88%20%3A%20%40farsiproxy",
            "vless://8c561eb2-f643-49ce-b5b6-81690ec268c0@b2n.ir:2087?mode=auto&path=%2FFiShChIpS&security=tls&encryption=none&extra=%7B%22mode%22%3A%22auto%22%2C%22xPaddingBytes%22%3A%221-1%22%2C%22xPaddingObfsMode%22%3Atrue%2C%22xPaddingKey%22%3A%22ctx%22%2C%22xPaddingHeader%22%3A%22x-grpc-context%22%2C%22xPaddingMethod%22%3A%22tokenish%22%2C%22sessionIDPlacement%22%3A%22header%22%2C%22sessionIDKey%22%3A%22Idempotency-Key%22%2C%22seqPlacement%22%3A%22header%22%2C%22seqKey%22%3A%22Upload-Offset%22%2C%22sessionPlacement%22%3A%22header%22%2C%22sessionKey%22%3A%22Idempotency-Key%22%7D&insecure=0&host=fish.kaftarkakolbesarwifi.ir&fp=chrome&type=xhttp&allowInsecure=0&sni=fish.kaftarkakolbesarwifi.ir#%D8%A7%DA%AF%D9%87%20%D9%85%DB%8C%D8%AE%D9%88%D8%A7%DB%8C%20%D9%82%D8%B7%D8%B9%20%D9%86%D8%B4%DB%8C%20%D8%AC%D9%88%DB%8C%D9%86%20%D8%B4%D9%88%20%3A%20%40farsiproxy",
        ];

        let res: Vec<IpAddr> = links
            .iter()
            .flat_map(|l| Fetcher::parse_link_to_ip(&l.to_string()))
            .collect();

        assert_eq!(res.len(), 5);
    }
}
