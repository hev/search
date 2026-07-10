//! hevsearch-api — axum REST frontend for the hevsearch core.
//!
//! The binary (`src/main.rs`) parses [`AppConfig`] from the
//! environment, builds an [`AppState`], and calls [`router`] to
//! mount the handlers. Tests reuse the same entry point by building
//! their own `AppState` (via `tests/common`) and driving [`router`]
//! through `tower::ServiceExt::oneshot`.

pub mod config;
pub mod error;
pub mod operations;
pub mod state;

mod handlers;

use axum::extract::DefaultBodyLimit;
use axum::routing::{delete, get, post};
use axum::Router;

pub use config::AppConfig;
pub use error::ApiError;
pub use state::{build_state, AppState};

/// Build the axum router wired to the application state.
/// The engine is an open internal service. Layer owns auth, tenancy,
/// and rate limiting; this router has no auth or limiter middleware.
pub fn router(state: AppState) -> Router {
    let body_limit = state.max_body_bytes;

    Router::new()
        .route("/health", get(handlers::health))
        .route("/metrics", get(handlers::metrics))
        .route("/ns/{namespace}/upsert", post(handlers::upsert))
        .route("/ns/{namespace}/query", post(handlers::query))
        .route("/ns/{namespace}/facet", post(handlers::facet))
        .route("/ns/{namespace}/list", get(handlers::list))
        .route("/ns/{namespace}/warmup", post(handlers::warmup))
        .route("/ns", get(handlers::list_namespaces))
        .route("/ns/{namespace}", get(handlers::info))
        .route("/operations/{operation_id}", get(handlers::get_operation))
        .merge(
            Router::new()
                .route("/ns/{namespace}/import", post(handlers::import))
                .layer(DefaultBodyLimit::disable()),
        )
        .route("/ns/{namespace}", delete(handlers::delete))
        .route("/ns/{namespace}/delete", post(handlers::delete_rows))
        .route("/ns/{namespace}/index", post(handlers::create_index))
        .route(
            "/ns/{namespace}/fts-index",
            post(handlers::create_fts_index),
        )
        .route(
            "/ns/{namespace}/scalar-index",
            post(handlers::create_scalar_index),
        )
        .route("/ns/{namespace}/compact", post(handlers::compact))
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(state)
}
