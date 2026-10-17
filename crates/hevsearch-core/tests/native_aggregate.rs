//! Native Lance physical-plan and semantic tests; isolated local storage.
use hevsearch_core::{
    aggregate::AggregateRequest, metrics::test_metrics, NamespaceId, NamespaceManager, RowId,
    StorageRoot, UpsertRow,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};

fn request(sql: &str) -> AggregateRequest {
    AggregateRequest {
        tables: BTreeMap::from([
            ("sessions".into(), "native-sessions".into()),
            ("evals".into(), "native-evals".into()),
        ]),
        sql: sql.into(),
        versions: BTreeMap::new(),
        max_rows: 100,
        max_bytes: 100_000,
        timeout_ms: 60_000,
        explain_only: false,
    }
}
fn row(id: &str, attrs: Value) -> UpsertRow {
    UpsertRow {
        id: RowId::String(id.into()),
        vector: vec![1.0, 0.0],
        vectors: None,
        text: None,
        attributes: attrs.as_object().unwrap().clone(),
    }
}
async fn fixture() -> (tempfile::TempDir, NamespaceManager) {
    let dir = tempfile::tempdir().unwrap();
    let m = NamespaceManager::new(
        StorageRoot::local(dir.path()).unwrap(),
        HashMap::new(),
        test_metrics(),
    );
    m.upsert(&NamespaceId::new("native-sessions").unwrap(), vec![
        row("a-old", json!({"session_id":"a", "end":10,"model":"wanted", "cost":99.0,"input":100,"cache_read":100,"cache_creation":100,"output":100,"prompts":["2026-11-01T05:30:00Z"]})),
        row("a", json!({"session_id":"a", "end":20,"model":"other", "cost":0.1,"input":1,"cache_read":2,"cache_creation":3,"output":4,"prompts":["2026-11-01T05:30:00Z","2026-11-01T06:30:00Z"]})),
        row("z-legacy", json!({"session_id":"a", "end":20,"model":"wanted", "cost":88.0,"input":100,"cache_read":100,"cache_creation":100,"output":100,"prompts":[]})),
        row("b-1", json!({"session_id":"b", "end":20,"model":"wanted", "cost":77.0,"input":100,"cache_read":100,"cache_creation":100,"output":100,"prompts":[]})),
        row("b-2", json!({"session_id":"b", "end":20,"model":"wanted", "cost":0.2,"input":10,"cache_read":20,"cache_creation":30,"output":40,"prompts":["2026-11-02T00:30:00Z"]})),
        row("c", json!({"session_id":"c", "end":30,"model":"wanted", "cost":0.0,"input":0,"cache_read":0,"cache_creation":0,"output":0,"prompts":[]})),
    ]).await.unwrap();
    m.upsert(
        &NamespaceId::new("native-evals").unwrap(),
        vec![
            row(
                "eval-a-old",
                json!({"session_id":"a","ts":1,"grade":"poor"}),
            ),
            row(
                "eval-a-new",
                json!({"session_id":"a","ts":2,"grade":"good"}),
            ),
            row("eval-b-1", json!({"session_id":"b","ts":2,"grade":"good"})),
            row("eval-b-2", json!({"session_id":"b","ts":2,"grade":"poor"})),
        ],
    )
    .await
    .unwrap();
    (dir, m)
}
const LATEST: &str = "WITH ranked AS (SELECT *, row_number() OVER (PARTITION BY session_id ORDER BY \"end\" DESC NULLS LAST, (id = session_id) DESC, id DESC) AS rn FROM sessions), latest AS (SELECT * FROM ranked WHERE rn = 1)";

#[tokio::test]
async fn latest_before_filter_canonical_and_legacy_ties_native_plan() {
    let (_dir, m) = fixture().await;
    let q = format!("{LATEST} SELECT id, session_id FROM latest WHERE model = 'wanted' ORDER BY \"end\" DESC, session_id ASC");
    let result = m.aggregate(&request(&q)).await.unwrap();
    assert_eq!(
        result.rows,
        vec![
            json!({"id":"c","session_id":"c"}),
            json!({"id":"b-2","session_id":"b"})
        ]
    );
    assert!(
        result.physical_plan.contains("Lance"),
        "{}",
        result.physical_plan
    );
    assert!(
        result.physical_plan.contains("Window"),
        "{}",
        result.physical_plan
    );
    assert!(
        result.physical_plan.contains("output_rows"),
        "{}",
        result.physical_plan
    );
    assert!(result.snapshots["sessions"].version > 0);
    assert_eq!(
        result.result_bytes,
        serde_json::to_vec(&result.rows).unwrap().len()
    );
}

#[tokio::test]
async fn latest_eval_anti_join_includes_ungraded() {
    let (_dir, m) = fixture().await;
    let q = format!("{LATEST}, eval_rank AS (SELECT session_id, grade, row_number() OVER (PARTITION BY session_id ORDER BY ts DESC, id DESC) AS n FROM evals), poor AS (SELECT session_id FROM eval_rank WHERE n = 1 AND grade = 'poor') SELECT l.session_id FROM latest l LEFT ANTI JOIN poor p ON l.session_id = p.session_id ORDER BY l.session_id");
    assert_eq!(
        m.aggregate(&request(&q)).await.unwrap().rows,
        vec![json!({"session_id":"a"}), json!({"session_id":"c"})]
    );
}

#[tokio::test]
async fn financial_reduction_separate_from_unnest_and_dst_heatmap() {
    let (_dir, m) = fixture().await;
    let q = format!("{LATEST} SELECT sum(CAST(cost AS DECIMAL(24,6))) AS spend, sum(input + output + cache_read + cache_creation) AS tokens, CAST(sum(cache_read) AS DOUBLE) / nullif(sum(input + cache_read + cache_creation),0) AS hit FROM latest");
    let result = m.aggregate(&request(&q)).await.unwrap();
    assert_eq!(result.rows[0]["spend"], json!("0.300000"));
    assert_eq!(result.rows[0]["tokens"], json!(110));
    assert!((result.rows[0]["hit"].as_f64().unwrap() - 1.0 / 3.0).abs() < 1e-10);
    assert!(result.physical_plan.contains("Aggregate"));
    let q = format!("{LATEST}, expanded AS (SELECT unnest(prompts) AS ts FROM latest), localized AS (SELECT to_local_time(to_timestamp(ts) AT TIME ZONE 'America/New_York') AS ts FROM expanded) SELECT date_part('dow',ts) AS dow, date_part('hour',ts) AS hour, count(*) AS n FROM localized GROUP BY dow,hour ORDER BY dow,hour");
    let result = m.aggregate(&request(&q)).await.unwrap();
    assert_eq!(
        result.rows,
        vec![
            json!({"dow":0.0,"hour":1.0,"n":2}),
            json!({"dow":0.0,"hour":19.0,"n":1})
        ]
    );
    assert!(result.physical_plan.contains("Unnest"));
}

#[tokio::test]
async fn budgets_explain_and_mutations_refuse_without_effects() {
    let (_dir, m) = fixture().await;
    let mut r = request("SELECT id FROM sessions");
    r.max_rows = 1;
    assert!(m
        .aggregate(&r)
        .await
        .unwrap_err()
        .to_string()
        .contains("max_rows"));
    r.max_rows = 100;
    r.max_bytes = 2;
    assert!(m
        .aggregate(&r)
        .await
        .unwrap_err()
        .to_string()
        .contains("max_bytes"));
    r.max_bytes = 100_000;
    r.explain_only = true;
    let result = m.aggregate(&r).await.unwrap();
    assert!(!result.executed);
    assert!(result.rows.is_empty());
    assert!(result.physical_plan.contains("Lance"));
    for sql in [
        "DELETE FROM sessions",
        "DROP TABLE sessions",
        "CREATE EXTERNAL TABLE stolen STORED AS CSV LOCATION '/etc/passwd'",
        "SET datafusion.execution.batch_size = 1",
        "COPY sessions TO '/tmp/forbidden'",
        "SELECT 1; SELECT 2",
        "EXPLAIN ANALYZE SELECT * FROM sessions",
    ] {
        assert!(m.aggregate(&request(sql)).await.is_err(), "accepted {sql}");
    }
    let result = m
        .aggregate(&request("SELECT count(*) AS n FROM sessions"))
        .await
        .unwrap();
    assert_eq!(result.rows, vec![json!({"n":6})]);
}
