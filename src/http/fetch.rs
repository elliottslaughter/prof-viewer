use bytes::Bytes;

#[cfg(target_arch = "wasm32")]
use reqwest::RequestBuilder;
#[cfg(not(target_arch = "wasm32"))]
use reqwest::blocking::RequestBuilder;

pub struct DataSourceResponse {
    pub body: Bytes,
}

pub fn fetch(
    request: RequestBuilder,
    on_done: impl 'static + Send + FnOnce(Result<DataSourceResponse, String>),
) {
    #[cfg(not(target_arch = "wasm32"))]
    crate::http::fetch_native::fetch(request, Box::new(on_done));

    #[cfg(target_arch = "wasm32")]
    crate::http::fetch_web::fetch(request, Box::new(on_done));
}
