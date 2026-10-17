//! Synthetic, isolated native-engine measurement. Never reads an archive.
//! cargo run -p hevsearch-core --example aggregate_probe -- [samples=30]
use hevsearch_core::{
    aggregate::AggregateRequest, metrics::test_metrics, NamespaceId, NamespaceManager, RowId,
    StorageRoot, UpsertRow,
};
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    time::Instant,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let samples: usize = std::env::args().nth(1).unwrap_or("30".into()).parse()?;
    if samples == 0 || samples > 100 {
        return Err("samples must be 1..=100".into());
    }
    let root = tempfile::tempdir()?;
    let make_manager = || {
        NamespaceManager::new(
            StorageRoot::local(root.path()).unwrap(),
            HashMap::new(),
            test_metrics(),
        )
    };
    let manager = make_manager();
    let ns = NamespaceId::new("synthetic-aggregate-probe")?;
    let mut rows = Vec::with_capacity(47_326);
    for i in 0..47_326 {
        let logical = i % 20_711;
        let canonical = i < 20_711;
        rows.push(UpsertRow { id:RowId::String(if canonical {format!("s{logical:05}")} else {format!("legacy-{i:05}")}),vector:vec![1.0,0.0],vectors:None,text:None,
            attributes:json!({"session_id":format!("s{logical:05}"),"end":if canonical {3} else {1},"model":format!("m{}",logical%4),"cost":0.01,"tokens":10,"prompt_ts":[1793511000000_i64,1793514600000_i64]}).as_object().unwrap().clone()});
    }
    let ingest_start = Instant::now();
    manager.upsert(&ns, rows).await?;
    let ingest_ms = ingest_start.elapsed().as_secs_f64() * 1000.0;
    let query = |nonce: usize| {
        AggregateRequest {
        tables:BTreeMap::from([("s".into(),ns.to_string())]),versions:BTreeMap::new(),
        sql:format!("WITH ranked AS (SELECT session_id, model, cost, tokens, row_number() OVER (PARTITION BY session_id ORDER BY \"end\" DESC, (id=session_id) DESC, id DESC) AS rn FROM s) SELECT model, count(*) AS sessions, sum(CAST(cost AS DECIMAL(24,6))) AS spend, sum(tokens) AS tokens FROM ranked WHERE rn=1 AND tokens < {} GROUP BY model ORDER BY model", 100+nonce),
        max_rows:4,max_bytes:4096,timeout_ms:60_000,explain_only:false,
    }
    };
    let start = Instant::now();
    let initial = manager.aggregate(&query(0)).await?;
    let startup_ms = start.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(
        initial
            .rows
            .iter()
            .map(|r| r["sessions"].as_u64().unwrap())
            .sum::<u64>(),
        20_711
    );
    assert_eq!(
        initial
            .rows
            .iter()
            .map(|r| r["tokens"].as_i64().unwrap())
            .sum::<i64>(),
        207_110
    );
    assert_eq!(initial.rows[0]["spend"], json!("51.780000"));
    let mut modes = BTreeMap::new();
    for mode in ["manager_cold", "novel_predicate_warm", "repeat_uncached"] {
        let mut raw = Vec::new();
        for sample in 0..samples {
            let start = Instant::now();
            let result = match mode {
                "manager_cold" => make_manager().aggregate(&query(0)).await?,
                "novel_predicate_warm" => manager.aggregate(&query(sample + 1)).await?,
                _ => manager.aggregate(&query(0)).await?,
            };
            raw.push(start.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(result.rows, initial.rows);
        }
        let mut sorted = raw.clone();
        sorted.sort_by(f64::total_cmp);
        modes.insert(mode,json!({"raw_ms":raw,"p50_ms":sorted[(samples-1)/2],"p95_ms":sorted[((samples*95).div_ceil(100)-1).min(samples-1)]}));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "scope":"synthetic-local-engine-only","logical_sessions":20711,"physical_rows":47326,
            "samples_per_mode":samples,"ingest_ms":ingest_ms,"ingest_rows_per_second":47326.0/(ingest_ms/1000.0),
            "startup_ms":startup_ms,"modes":modes,"result":initial,
            "reset":"isolated temporary namespace; manager_cold creates a new manager but OS caches remain warm; aggregate results are not cached",
            "unmeasured":["live archive scale","provider bytes/cost","ingestion under query concurrency","source backend comparison","OS page-cache cold"]
        }))?
    );
    Ok(())
}
