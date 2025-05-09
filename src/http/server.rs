use std::sync::Arc;

use actix_cors::Cors;
use actix_web::{
    App, HttpServer, Responder, Result, error, get, http, middleware,
    web::{self, Data},
};

use serde::Serialize;

use crate::data::DataSource;
use crate::http::schema::{TileQuery, TileRequestPath};

struct AppState {
    data_source: Box<dyn DataSource + Send + Sync + 'static>,
}

pub struct DataSourceHTTPServer {
    host: String,
    port: u16,
    state: AppState,
}

fn encode<T>(data: T) -> Result<Vec<u8>>
where
    T: Serialize,
{
    let mut f = zstd::Encoder::new(Vec::new(), 1)?;
    ciborium::into_writer(&data, &mut f).expect("ciborium encoding failed");
    let f = f.finish()?;
    Ok(f)
}

#[get("/info")]
async fn fetch_info(state: web::Data<AppState>) -> Result<impl Responder> {
    let result = state.data_source.fetch_info();
    encode(result)
}

#[get("/summary_tile/{entry_id}/{tile_id}")]
async fn fetch_summary_tile(
    path: web::Path<TileRequestPath>,
    query: web::Query<TileQuery>,
    state: web::Data<AppState>,
) -> Result<impl Responder> {
    let path = path
        .parse()
        .map_err(|e| error::ErrorBadRequest(format!("bad request: {}", e)))?;
    let result = state
        .data_source
        .fetch_summary_tile(&path.entry_id, path.tile_id, query.full);
    encode(result)
}

#[get("/slot_tile/{entry_id}/{tile_id}")]
async fn fetch_slot_tile(
    path: web::Path<TileRequestPath>,
    query: web::Query<TileQuery>,
    state: web::Data<AppState>,
) -> Result<impl Responder> {
    let path = path
        .parse()
        .map_err(|e| error::ErrorBadRequest(format!("bad request: {}", e)))?;
    let result = state
        .data_source
        .fetch_slot_tile(&path.entry_id, path.tile_id, query.full);
    encode(result)
}

#[get("/slot_meta_tile/{entry_id}/{tile_id}")]
async fn fetch_slot_meta_tile(
    path: web::Path<TileRequestPath>,
    query: web::Query<TileQuery>,
    state: web::Data<AppState>,
) -> Result<impl Responder> {
    let path = path
        .parse()
        .map_err(|e| error::ErrorBadRequest(format!("bad request: {}", e)))?;
    let result = state
        .data_source
        .fetch_slot_meta_tile(&path.entry_id, path.tile_id, query.full);
    encode(result)
}

impl DataSourceHTTPServer {
    pub fn new(
        host: String,
        port: u16,
        data_source: Box<dyn DataSource + Send + Sync + 'static>,
    ) -> Self {
        Self {
            host,
            port,
            state: AppState { data_source },
        }
    }

    #[actix_web::main]
    pub async fn run(self) -> std::io::Result<()> {
        let state = Data::from(Arc::new(self.state));
        HttpServer::new(move || {
            let cors = Cors::default()
                .send_wildcard()
                .allow_any_origin()
                .allowed_methods(vec!["GET", "POST"])
                .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
                .allowed_header(http::header::CONTENT_TYPE)
                .max_age(3600);
            App::new()
                .wrap(middleware::Logger::default())
                .wrap(cors)
                .app_data(state.clone())
                .service(fetch_info)
                .service(fetch_summary_tile)
                .service(fetch_slot_tile)
                .service(fetch_slot_meta_tile)
        })
        .bind((self.host.as_str(), self.port))?
        .run()
        .await
    }
}
