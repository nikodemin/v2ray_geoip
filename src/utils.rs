use clokwerk::Interval;
use serde::{Deserialize, Deserializer};
use std::fmt::Formatter;
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct Wrapper<T>(pub T);

impl<'de> Deserialize<'de> for Wrapper<Duration> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Duration;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str(
                    "Duration must follow pattern '{num} {unit}', where {unit} is one of h,m,s",
                )
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let mut iter = v.split(' ');
                let num = match iter.next().map(|n| n.parse::<u64>()) {
                    Some(Ok(num)) => num,
                    _ => {
                        return Err(serde::de::Error::invalid_value(
                            serde::de::Unexpected::Str(v),
                            &self,
                        ));
                    }
                };
                match iter.next() {
                    Some("h") => Ok(Duration::from_hours(num)),
                    Some("m") => Ok(Duration::from_mins(num)),
                    Some("s") => Ok(Duration::from_secs(num)),
                    _ => Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Str(v),
                        &self,
                    )),
                }
            }
        }
        deserializer.deserialize_str(Visitor).map(Wrapper)
    }
}

impl Into<Interval> for Wrapper<Duration> {
    fn into(self) -> Interval {
        Interval::Seconds(self.0.as_secs_f32() as u32)
    }
}

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

pub fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}

pub fn write_config() -> std::io::Result<()> {
    fs::exists("conf.toml")
        .and_then(|conf_exists| {
            if conf_exists {
                Ok(())
            } else {
                fs::write(
                    "conf.toml",
                    r#"sub_groups = [
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub1.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub2.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub3.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub4.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub5.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub6.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub7.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub8.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub9.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub10.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub11.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub12.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub13.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub14.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub15.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/Sub16.txt",
]
geo_base_url = "http://ip-api.com"
batch_size = 50
update_period = "1 h"
recheck_period = "10 m"
retries = 7
port = 3000"#,
                )
            }
        })
        .and_then(|_| {
            fs::exists("log4rs.yml").and_then(|log_exists| {
                if log_exists {
                    Ok(())
                } else {
                    fs::write(
                        "log4rs.yml",
                        r#"appenders:
  stdout:
    kind: console
    encoder:
      pattern: "{d} - {l} - {f}:{L} - {m}{n}"
  file:
    kind: rolling_file
    path: "log/app.log"
    encoder:
      pattern: "{d} - {l} - {f}:{L} - {m}{n}"
    policy:
      trigger:
        kind: size
        limit: 100 mb
      roller:
        kind: delete
root:
  level: info
  appenders:
    - stdout
    - file"#,
                    )
                }
            })
        })
}
