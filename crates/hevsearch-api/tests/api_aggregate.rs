//! Exercise the real router over isolated local Lance storage, without MinIO.
mod common;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use hevsearch_api::{operations::OperationRegistry, router, AppState};
use hevsearch_core::{
    cache::NamespaceCache, metrics::test_metrics, NamespaceManager, NamespaceService, StorageRoot,
};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
use tower::ServiceExt;

async fn call(app: &axum::Router, method: &str, path: &str, payload: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 10 * 1024 * 1024)
        .await
        .unwrap();
    (
        status,
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        },
    )
}

#[tokio::test]
async fn schema_metadata_first_aggregate_vector_completion_and_refusals() {
    let data = tempfile::tempdir().unwrap();
    let cache_dir = tempfile::tempdir().unwrap();
    let metrics = test_metrics();
    let manager = Arc::new(NamespaceManager::new(
        StorageRoot::local(data.path()).unwrap(),
        HashMap::new(),
        Arc::clone(&metrics),
    ));
    let cache = Arc::new(
        NamespaceCache::new(
            16 * 1024 * 1024,
            cache_dir.path(),
            64 * 1024 * 1024,
            Arc::clone(&metrics),
        )
        .await
        .unwrap(),
    );
    let app = router(AppState {
        service: Arc::new(NamespaceService::new(
            Arc::clone(&manager),
            cache,
            Arc::clone(&metrics),
        )),
        manager,
        metrics,
        operations: Arc::new(OperationRegistry::new()),
        max_body_bytes: 32 * 1024 * 1024,
        import_max_bytes: 0,
        import_tmp_dir: data.path().to_path_buf(),
    });
    let schema = json!({"dim":2,"kind":"single","id_type":"string","distance_metric":"cosine","attributes":{"session_id":"utf8","prompt_ts":"int64_list","cost":"float64","pending":"boolean"}});
    assert_eq!(
        call(&app, "POST", "/ns/api-native/schema", schema.clone())
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(&app, "GET", "/ns/api-native/schema", Value::Null)
            .await
            .1,
        schema
    );
    let merged=call(&app,"POST","/ns/api-native/merge",json!({"rows":[{"id":"guarded","vector":[1.0,0.0],"text":"New Phrase","attributes":{"session_id":"guarded","cost":0.5,"pending":true}}],"condition":"false","insert_if_missing":true})).await;
    assert_eq!(merged.0, StatusCode::OK);
    assert_eq!(merged.1["inserted"], 1);
    let patched=call(&app,"POST","/ns/api-native/patch",json!({"rows":[{"id":"guarded","set":{"cost":0.75,"pending":false}}],"condition":"pending = true"})).await;
    assert_eq!(patched.0, StatusCode::OK);
    assert_eq!(patched.1["patched"], 1);
    assert_eq!(patched.1["affected_ids"], json!(["guarded"]));
    let skipped = call(
        &app,
        "POST",
        "/ns/api-native/patch",
        json!({"rows":[{"id":"guarded","set":{"cost":99.0}}],"condition":"pending = true"}),
    )
    .await;
    assert_eq!(skipped.1["patched"], 0);
    // Remove the fixture through the existing supported delete route so the
    // remaining metadata-first assertions still address an empty namespace.
    assert_eq!(
        call(
            &app,
            "POST",
            "/ns/api-native/delete",
            json!({"ids":["guarded"]})
        )
        .await
        .0,
        StatusCode::OK
    );
    let r = json!({"tables":{"s":"api-native"},"sql":"SELECT count(*) AS n FROM s","max_rows":10,"max_bytes":10000,"timeout_ms":60000});
    assert_eq!(
        call(&app, "POST", "/aggregate", r.clone()).await.1["rows"],
        json!([{"n":0}])
    );
    let metadata = json!({"rows":[{"id":"a","attributes":{"session_id":"a","prompt_ts":[1793511000000_i64,null,1793514600000_i64],"cost":0.2,"pending":true}}]});
    let (status, body) = call(&app, "POST", "/ns/api-native/upsert", metadata).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut inspect = r.clone();
    inspect["sql"] = json!("SELECT id, vector, prompt_ts, cost, pending FROM s");
    let (status, body) = call(&app, "POST", "/aggregate", inspect.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["rows"],
        json!([{"id":"a","vector":null,"prompt_ts":[1793511000000_i64,null,1793514600000_i64],"cost":0.2,"pending":true}])
    );
    let (status, pending_query) = call(
        &app,
        "POST",
        "/ns/api-native/query",
        json!({"vector":[1.0,0.0],"k":10}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{pending_query}");
    assert_eq!(pending_query["results"], json!([]));
    let complete = json!({"rows":[{"id":"a","vector":[0.5,0.25],"attributes":{"session_id":"a","prompt_ts":[1793511000000_i64],"cost":0.2,"pending":false}}]});
    assert_eq!(
        call(&app, "POST", "/ns/api-native/upsert", complete)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, "POST", "/aggregate", inspect).await.1["rows"][0]["vector"],
        json!([0.5, 0.25])
    );
    assert_eq!(
        call(&app, "POST", "/aggregate", r.clone()).await.1["rows"],
        json!([{"n":1}])
    );
    let mut bad_schema = schema.clone();
    bad_schema["attributes"]["cost"] = json!("utf8");
    bad_schema["attributes"]["a_new_column"] = json!("utf8");
    assert_eq!(
        call(&app, "POST", "/ns/api-native/schema", bad_schema)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&app, "GET", "/ns/api-native/schema", Value::Null)
            .await
            .1,
        schema
    );
    let mut mutate = r.clone();
    mutate["sql"] = json!("DELETE FROM s");
    assert_eq!(
        call(&app, "POST", "/aggregate", mutate).await.0,
        StatusCode::BAD_REQUEST
    );
    let mut invalid = r.clone();
    invalid["max_rows"] = json!(0);
    assert_eq!(
        call(&app, "POST", "/aggregate", invalid).await.0,
        StatusCode::BAD_REQUEST
    );
    let mut missing = r;
    missing["tables"] = json!({"s":"does-not-exist"});
    assert_eq!(
        call(&app, "POST", "/aggregate", missing).await.0,
        StatusCode::BAD_REQUEST
    );
}
