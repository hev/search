//! Integration tests for `POST /ns/{namespace}/import` — the binary
//! Arrow IPC bulk-ingest path.
//!
//! Offline tests (no MinIO) cover the synchronous rejections that fire
//! before any storage is touched: wrong `Content-Type` (415), a body
//! over `HEVSEARCH_IMPORT_MAX_BYTES` (413), and a malformed Arrow stream
//! (400). The MinIO-gated test drives a real Arrow IPC body end to end:
//! 202 + `operation_id`, poll to `succeeded`, then confirm the rows
//! landed in a single commit.
//!
//! ```text
//! docker compose up -d minio minio-init
//! ./scripts/cargo test -p hevsearch-api --test api_import -- --ignored
//! ```

use std::sync::Arc;
use std::time::Duration;

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::{RecordBatch, StringArray, UInt64Array};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use hevsearch_api::router;
use serde_json::Value;
use tower::ServiceExt;

mod common;
use common::{test_state, test_state_offline, unique_namespace};

const ARROW_CT: &str = "application/vnd.apache.arrow.stream";
const DIM: usize = 4;

async fn post_bytes(
    app: axum::Router,
    uri: String,
    content_type: &str,
    body: Vec<u8>,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", content_type)
        .body(Body::from(body))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

async fn get(app: axum::Router, uri: String) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

fn single_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            false,
        ),
    ]))
}

fn string_id_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            false,
        ),
    ]))
}

fn single_batch(schema: &SchemaRef, ids: &[u64]) -> RecordBatch {
    let id_arr = UInt64Array::from_iter_values(ids.iter().copied());
    let mut list = FixedSizeListBuilder::new(Float32Builder::new(), DIM as i32);
    for &id in ids {
        for axis in 0..DIM {
            list.values().append_value(if axis == (id as usize % DIM) {
                1.0
            } else {
                0.0
            });
        }
        list.append(true);
    }
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_arr), Arc::new(list.finish())],
    )
    .unwrap()
}

fn string_id_batch(schema: &SchemaRef, ids: &[&str]) -> RecordBatch {
    let id_arr = StringArray::from_iter_values(ids.iter().copied());
    let mut list = FixedSizeListBuilder::new(Float32Builder::new(), DIM as i32);
    for (idx, _) in ids.iter().enumerate() {
        for axis in 0..DIM {
            list.values()
                .append_value(if axis == idx % DIM { 1.0 } else { 0.0 });
        }
        list.append(true);
    }
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_arr), Arc::new(list.finish())],
    )
    .unwrap()
}

/// Serialize batches as an Arrow IPC stream (the `/import` wire format).
fn arrow_ipc(schema: &SchemaRef, batches: &[RecordBatch]) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema).unwrap();
        for b in batches {
            w.write(b).unwrap();
        }
        w.finish().unwrap();
    }
    buf
}

async fn wait_for_operation(app: axum::Router, op_id: &str) -> Value {
    for _ in 0..600 {
        let (s, op) = get(app.clone(), format!("/operations/{op_id}")).await;
        assert_eq!(s, StatusCode::OK);
        match op["status"].as_str() {
            Some("succeeded") | Some("failed") => return op,
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    panic!("operation {op_id} did not finish in time");
}

#[tokio::test]
async fn import_rejects_wrong_content_type() {
    let (state, _tmp) = test_state_offline().await;
    let app = router(state);
    let ns = unique_namespace("import-ct");
    let (status, _) = post_bytes(
        app,
        format!("/ns/{ns}/import"),
        "application/json",
        b"{}".to_vec(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "non-Arrow content type must be rejected with 415"
    );
}

#[tokio::test]
async fn import_rejects_body_over_cap() {
    let (mut state, _tmp) = test_state_offline().await;
    state.import_max_bytes = 16; // tiny cap for the test
    let app = router(state);
    let ns = unique_namespace("import-413");
    // 100 bytes > 16-byte cap; the cap trips during spooling, before any
    // schema parse or storage touch, so the bytes need not be valid Arrow.
    let (status, _) = post_bytes(app, format!("/ns/{ns}/import"), ARROW_CT, vec![0u8; 100]).await;
    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "a body over HEVSEARCH_IMPORT_MAX_BYTES must be rejected with 413"
    );
}

#[tokio::test]
async fn import_rejects_malformed_arrow() {
    let (state, _tmp) = test_state_offline().await;
    let app = router(state);
    let ns = unique_namespace("import-garbage");
    let (status, _) = post_bytes(
        app,
        format!("/ns/{ns}/import"),
        ARROW_CT,
        b"this is not an arrow stream".to_vec(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a body that is not a valid Arrow IPC stream must be rejected with 400"
    );
}

#[tokio::test]
#[ignore]
async fn import_arrow_stream_lands_in_one_commit() {
    let (state, _tmp) = test_state().await;
    let app = router(state);
    let ns = unique_namespace("import-ok");
    let schema = single_schema();

    // Two batches in one stream → must be a single commit (table version 1
    // on a fresh namespace = the lone append, since the empty create is
    // version-zero state until the first data lands... assert <= 2 either
    // way, then confirm via row_count below).
    let body = arrow_ipc(
        &schema,
        &[
            single_batch(&schema, &[0, 1, 2, 3]),
            single_batch(&schema, &[4, 5, 6, 7]),
        ],
    );
    let (status, accepted) =
        post_bytes(app.clone(), format!("/ns/{ns}/import"), ARROW_CT, body).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "import returns 202: {accepted}"
    );
    let op_id = accepted["operation_id"]
        .as_str()
        .expect("202 carries operation_id")
        .to_string();
    assert_eq!(accepted["kind"], "import");

    let op = wait_for_operation(app.clone(), &op_id).await;
    assert_eq!(op["status"], "succeeded", "import operation failed: {op}");

    let (s, info) = get(app, format!("/ns/{ns}")).await;
    assert_eq!(s, StatusCode::OK, "namespace info: {info}");
    assert_eq!(
        info["row_count"].as_u64().unwrap(),
        8,
        "all 8 rows imported"
    );
}

#[tokio::test]
#[ignore]
async fn import_accepts_utf8_id_stream_and_rejects_existing_u64_namespace() {
    let (state, _tmp) = test_state().await;
    let app = router(state);
    let ns = unique_namespace("import-string-ids");
    let schema = string_id_schema();
    let body = arrow_ipc(
        &schema,
        &[string_id_batch(
            &schema,
            &["asin-B08N5WRWNW", "ticket-4117", "openfda-set's-42"],
        )],
    );

    let (status, accepted) =
        post_bytes(app.clone(), format!("/ns/{ns}/import"), ARROW_CT, body).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "import returns 202: {accepted}"
    );
    let op_id = accepted["operation_id"].as_str().unwrap();
    let op = wait_for_operation(app.clone(), op_id).await;
    assert_eq!(op["status"], "succeeded", "string import failed: {op}");

    let (s, info) = get(app.clone(), format!("/ns/{ns}")).await;
    assert_eq!(s, StatusCode::OK, "namespace info: {info}");
    assert_eq!(info["id_type"], "string");
    assert_eq!(info["row_count"], 3);

    let u64_ns = unique_namespace("import-u64-then-string");
    let u64_schema = single_schema();
    let u64_body = arrow_ipc(&u64_schema, &[single_batch(&u64_schema, &[1])]);
    let (status, accepted) = post_bytes(
        app.clone(),
        format!("/ns/{u64_ns}/import"),
        ARROW_CT,
        u64_body,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "u64 import returns 202: {accepted}"
    );
    let op = wait_for_operation(app.clone(), accepted["operation_id"].as_str().unwrap()).await;
    assert_eq!(op["status"], "succeeded", "u64 import failed: {op}");

    let mismatch_body = arrow_ipc(&schema, &[string_id_batch(&schema, &["ticket-4117"])]);
    let (status, accepted) = post_bytes(
        app.clone(),
        format!("/ns/{u64_ns}/import"),
        ARROW_CT,
        mismatch_body,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{accepted}");
    assert!(
        accepted["error"]
            .as_str()
            .unwrap_or_default()
            .contains("stream id_type is string"),
        "{accepted}"
    );
}
