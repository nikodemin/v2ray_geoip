use clokwerk::Interval;
use serde::{Deserialize, Deserializer};
use std::fmt::Formatter;
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
