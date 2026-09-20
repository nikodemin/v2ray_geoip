use crate::dao::{Dao, DaoOps};
use axum::Json;
use axum::extract::{Query, State};
use axum::http::Response;
use log::warn;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Deserialize)]
pub struct SubParams {
    country_code: Option<String>,
    city: Option<String>,
    page: Option<u32>,
    limit: u32,
}

#[derive(Serialize)]
pub struct DictResponse(HashMap<String, HashSet<String>>);

#[derive(Clone)]
pub struct Api {
    dao: Arc<Dao>,
}

impl Api {
    pub fn new(dao: Arc<Dao>) -> Self {
        Self { dao }
    }

    pub async fn get_cc_to_city(State(api): State<Api>) -> Json<DictResponse> {
        match api.dao.get_county_codes_to_cities().await {
            Ok(res) => Json(DictResponse(res)),
            Err(err) => {
                warn!("Failed to retrieve dict from dao: {}", err);
                Json(DictResponse(HashMap::new()))
            }
        }
    }

    pub async fn get_subs(State(api): State<Api>, params: Query<SubParams>) -> Response<String> {
        match (params.country_code.clone(), params.city.clone()) {
            (Some(cc), None) => {
                match api
                    .dao
                    .list_by_country_code(cc, params.limit, params.page)
                    .await
                {
                    Ok(res) => {
                        let resp = res
                            .into_iter()
                            .fold(String::new(), |acc, e| acc + "\n" + e.url.as_str());
                        Response::builder().status(200).body(resp).unwrap()
                    }
                    Err(err) => {
                        warn!("Failed to retrieve subs from dao: {}", err);
                        Response::builder()
                            .status(500)
                            .body("Failed to retrieve subs from dao".to_string())
                            .unwrap()
                    }
                }
            }
            (Some(cc), Some(city)) => {
                match api
                    .dao
                    .list_by_country_code_and_city(cc, city, params.limit, params.page)
                    .await
                {
                    Ok(res) => {
                        let resp = res
                            .into_iter()
                            .fold(String::new(), |acc, e| acc + "\n" + e.url.as_str());
                        Response::builder().status(200).body(resp).unwrap()
                    }
                    Err(err) => {
                        warn!("Failed to retrieve subs from dao: {}", err);
                        Response::builder()
                            .status(500)
                            .body("Failed to retrieve subs from dao".to_string())
                            .unwrap()
                    }
                }
            }
            (_, _) => match api.dao.list(params.limit, params.page).await {
                Ok(res) => {
                    let resp = res
                        .into_iter()
                        .fold(String::new(), |acc, e| acc + "\n" + e.url.as_str());
                    Response::builder().status(200).body(resp).unwrap()
                }
                Err(err) => {
                    warn!("Failed to retrieve subs from dao: {}", err);
                    Response::builder()
                        .status(500)
                        .body("Failed to retrieve subs from dao".to_string())
                        .unwrap()
                }
            },
        }
    }
}
