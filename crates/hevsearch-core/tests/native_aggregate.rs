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
    let q = format!("{LATEST}, expanded AS (SELECT unnest(prompts) AS ts FROM latest), localized AS (SELECT to_local_time(arrow_cast(CAST(to_timestamp(ts) AS BIGINT), 'Timestamp(Nanosecond, Some(\"America/New_York\"))')) AS ts FROM expanded) SELECT date_part('dow',ts) AS dow, date_part('hour',ts) AS hour, count(*) AS n FROM localized GROUP BY dow,hour ORDER BY dow,hour");
    let result = m.aggregate(&request(&q)).await.unwrap();
    assert_eq!(
        result.rows,
        vec![
            json!({"dow":0,"hour":1,"n":2}),
            json!({"dow":0,"hour":19,"n":1})
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

#[tokio::test]
async fn explicit_versions_repeat_after_write_and_shared_aliases() {
    let (_dir, m) = fixture().await;
    let r = request("SELECT count(*) AS n FROM sessions");
    let initial = m.aggregate(&r).await.unwrap();
    m.upsert(
        &NamespaceId::new("native-sessions").unwrap(),
        vec![row("new", json!({"session_id":"new","end":99}))],
    )
    .await
    .unwrap();
    assert_eq!(
        m.aggregate(&r).await.unwrap().rows,
        json!([{"n":7}]).as_array().unwrap().clone()
    );
    let mut pinned = r.clone();
    pinned.versions.insert(
        "native-sessions".into(),
        initial.snapshots["sessions"].version,
    );
    pinned
        .tables
        .insert("duplicate".into(), "native-sessions".into());
    let result = m.aggregate(&pinned).await.unwrap();
    assert_eq!(result.rows, initial.rows);
    assert_eq!(
        result.snapshots["sessions"].version,
        result.snapshots["duplicate"].version
    );
    pinned.versions.insert("native-sessions".into(), u64::MAX);
    assert!(m.aggregate(&pinned).await.is_err());
}

#[tokio::test]
async fn any_prompt_qualifies_session_then_all_prompts_are_aggregated() {
    let (_dir, m) = fixture().await;
    let sql=format!("{LATEST}, expanded AS (SELECT session_id, unnest(prompts) AS ts FROM latest), selected AS (SELECT DISTINCT session_id FROM expanded WHERE to_timestamp(ts) < to_timestamp('2026-11-01T06:00:00Z')) SELECT count(*) AS n FROM expanded e LEFT SEMI JOIN selected s ON e.session_id=s.session_id");
    assert_eq!(
        m.aggregate(&request(&sql)).await.unwrap().rows,
        vec![json!({"n":2})]
    );
    let invalid=request("SELECT to_local_time(to_timestamp('2026-11-01T05:30:00Z') AT TIME ZONE 'Invalid/Zone') AS x");
    assert!(m.aggregate(&invalid).await.is_err());
}

#[tokio::test]
async fn deadline_cancels_native_work_and_next_query_succeeds() {
    let (_dir, m) = fixture().await;
    let mut q=request("SELECT sum(CAST(value AS DOUBLE)*value) AS n FROM generate_series(1,1000000000) AS x(value)");
    q.timeout_ms = 20;
    let start = std::time::Instant::now();
    let error = m.aggregate(&q).await.unwrap_err().to_string();
    assert!(error.contains("deadline"), "{error}");
    assert!(start.elapsed() < std::time::Duration::from_secs(2));
    assert_eq!(
        m.aggregate(&request("SELECT count(*) AS n FROM sessions"))
            .await
            .unwrap()
            .rows,
        vec![json!({"n":6})]
    );
}

#[tokio::test]
async fn missing_metrics_and_zero_denominators_keep_usage_and_zero_ratios() {
    let (_dir, m) = fixture().await;
    // Source-style omitted metrics must not null out other fields in the row.
    m.upsert(
        &NamespaceId::new("native-sessions").unwrap(),
        vec![row(
            "a",
            json!({"session_id":"a","end":100,"input":5,"cost":0.1}),
        )],
    )
    .await
    .unwrap();
    let sql=format!("{LATEST} SELECT sum(coalesce(input,0)+coalesce(output,0)+coalesce(cache_read,0)+coalesce(cache_creation,0)) AS tokens FROM latest WHERE session_id='a'");
    assert_eq!(
        m.aggregate(&request(&sql)).await.unwrap().rows,
        vec![json!({"tokens":5})]
    );
    for predicate in ["session_id='c'", "session_id='missing'"] {
        let sql=format!("{LATEST} SELECT coalesce(sum(CAST(cost AS DECIMAL(24,6))), CAST(0 AS DECIMAL(24,6))) AS spend, coalesce(CAST(sum(coalesce(cache_read,0)) AS DOUBLE)/nullif(sum(coalesce(input,0)+coalesce(cache_read,0)+coalesce(cache_creation,0)),0),0.0) AS hit FROM latest WHERE {predicate}");
        let result = m.aggregate(&request(&sql)).await.unwrap();
        assert_eq!(result.rows[0]["spend"], json!("0.000000"));
        assert_eq!(result.rows[0]["hit"].as_f64(), Some(0.0));
    }
}

#[tokio::test]
async fn phrase_predicate_runs_natively_before_bounded_projection() {
    let (_dir, m) = fixture().await;
    m.upsert(
        &NamespaceId::new("native-sessions").unwrap(),
        vec![
            UpsertRow {
                text: Some("Café,\u{a0}finish NOW".into()),
                ..row("phrase-yes", json!({"session_id":"phrase","end":2}))
            },
            UpsertRow {
                text: Some("Café middle finish".into()),
                ..row("phrase-no", json!({"session_id":"phrase","end":1}))
            },
            row("no-text", json!({"session_id":"phrase","end":0})),
        ],
    )
    .await
    .unwrap();
    let result=m.aggregate(&request("SELECT id FROM sessions WHERE session_id='phrase' AND phrase_matches(text, 'CAFÉ, FINISH') ORDER BY id")).await.unwrap();
    assert_eq!(result.rows, vec![json!({"id":"phrase-yes"})]);
    assert!(result.physical_plan.contains("phrase_matches"));
    let result=m.aggregate(&request("SELECT phrase_matches(NULL, 'anything') AS missing, phrase_matches('foo', '') AS empty, phrase_matches('foobar','foo') AS substring")).await.unwrap();
    assert_eq!(
        result.rows,
        vec![json!({"missing":false,"empty":false,"substring":true})]
    );
}
