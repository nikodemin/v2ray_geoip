use std::fmt::Formatter;
use std::num::ParseIntError;
use clokwerk::Interval;
use serde::{Deserialize, Deserializer};

pub struct Wrapper<T>(T);

impl<'de> Deserialize<'de> for Wrapper<Interval> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Interval;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("interval must follow pattern '{num} {unit}', where {unit} is one of h,m,s")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let mut iter = v.split(' ');
                let num = match iter.next().map(|n|n.parse::<u32>()) {
                    Some(Ok(num)) => num,
                    _ => return Err(serde::de::Error::invalid_value(serde::de::Unexpected::Str(v), &self)),
                };
                match iter.next() {
                    Some("h") => Ok(Interval::Hours(num)),
                    Some("m") => Ok(Interval::Minutes(num)),
                    Some("s") => Ok(Interval::Seconds(num)),
                    _ => Err(serde::de::Error::invalid_value(serde::de::Unexpected::Str(v), &self)),
                }
            }
        }
        deserializer.deserialize_str(Visitor).map(Wrapper)
    }
}
