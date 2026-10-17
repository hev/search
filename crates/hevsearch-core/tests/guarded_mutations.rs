//! Native atomic writes over isolated local Lance tables.
use hevsearch_core::{
    aggregate::AggregateRequest,
    manager::{AttributeType, GuardedMergeRequest, NamespaceSchema, PatchRequest, PatchRow},
    metrics::test_metrics,
    DistanceMetric, HevSearchError, NamespaceId, NamespaceManager, RowId, RowIdType, StorageRoot,
    UpsertRow, VectorKind,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

fn row(id: &str, version: i64, summary: &str) -> UpsertRow {
    UpsertRow {id:RowId::String(id.into()),vector:vec![1.,0.,0.],vectors:None,text:Some("Old Café, finish".into()),attributes:json!({"version":version,"epoch":"one","summary":summary,"tags":["a","b"],"pending":false}).as_object().unwrap().clone()}
}
async fn fixture() -> (tempfile::TempDir, Arc<NamespaceManager>, NamespaceId) {
    let dir = tempfile::tempdir().unwrap();
    let m = Arc::new(NamespaceManager::new(
        StorageRoot::local(dir.path()).unwrap(),
        HashMap::new(),
        test_metrics(),
    ));
    let ns = NamespaceId::new("guarded-fixture").unwrap();
    m.ensure_schema(
        &ns,
        &NamespaceSchema {
            dim: 3,
            kind: VectorKind::Single,
            id_type: RowIdType::String,
            distance_metric: DistanceMetric::Cosine,
            attributes: BTreeMap::from([
                ("version".into(), AttributeType::Int64),
                ("epoch".into(), AttributeType::Utf8),
                ("summary".into(), AttributeType::Utf8),
                ("tags".into(), AttributeType::StringList),
                ("pending".into(), AttributeType::Boolean),
            ]),
        },
    )
    .await
    .unwrap();
    m.upsert(&ns, vec![row("a", 1, "original"), row("b", 1, "second")])
        .await
        .unwrap();
    (dir, m, ns)
}
async fn read(m: &NamespaceManager, ns: &NamespaceId) -> Vec<Value> {
    m.aggregate(&AggregateRequest {
        tables: BTreeMap::from([("rows".into(), ns.to_string())]),
        versions: BTreeMap::new(),
        sql: "SELECT id,vector,text,version,epoch,summary,tags,pending FROM rows ORDER BY id"
            .into(),
        max_rows: 100,
        max_bytes: 100_000,
        timeout_ms: 5000,
        explain_only: false,
    })
    .await
    .unwrap()
    .rows
}
fn patch(id: &str, values: Value, condition: Option<&str>) -> PatchRequest {
    PatchRequest {
        rows: vec![PatchRow {
            id: RowId::String(id.into()),
            set: values
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }],
        condition: condition.map(str::to_string),
        timeout_ms: 5000,
    }
}
fn merge(rows: Vec<UpsertRow>, condition: &str, insert_if_missing: bool) -> GuardedMergeRequest {
    GuardedMergeRequest {
        rows,
        condition: condition.into(),
        insert_if_missing,
        timeout_ms: 5000,
    }
}

#[tokio::test]
async fn patch_preserves_unspecified_fields_and_reports_exact_conditional_ids() {
    let (_dir, m, ns) = fixture().await;
    let before = read(&m, &ns).await;
    let result = m
        .patch(
            &ns,
            &patch(
                "a",
                json!({"summary":"O'Reilly\nnew","version":2}),
                Some("version = 1"),
            ),
        )
        .await
        .unwrap();
    assert_eq!(result.patched, 1);
    assert_eq!(result.affected_ids, vec![RowId::String("a".into())]);
    let mut expected = before.clone();
    expected[0]["summary"] = json!("O'Reilly\nnew");
    expected[0]["version"] = json!(2);
    assert_eq!(read(&m, &ns).await, expected);
    let skip = m
        .patch(
            &ns,
            &patch("a", json!({"summary":"stale"}), Some("version = 1")),
        )
        .await
        .unwrap();
    assert_eq!(skip.patched, 0);
    assert!(skip.affected_ids.is_empty());
    assert_eq!(read(&m, &ns).await, expected);
    let req = PatchRequest {
        rows: vec![
            PatchRow {
                id: RowId::String("a".into()),
                set: BTreeMap::from([("summary".into(), json!("a-new"))]),
            },
            PatchRow {
                id: RowId::String("b".into()),
                set: BTreeMap::from([("summary".into(), json!("b-new"))]),
            },
        ],
        condition: Some("version = 1".into()),
        timeout_ms: 5000,
    };
    let result = m.patch(&ns, &req).await.unwrap();
    assert_eq!(result.affected_ids, vec![RowId::String("b".into())]);
    assert_eq!(read(&m, &ns).await[0]["summary"], expected[0]["summary"]);
}

#[tokio::test]
async fn guarded_postimages_insert_only_and_reject_older_versions() {
    let (_dir, m, ns) = fixture().await;
    let req = merge(
        vec![row("a", 0, "wrong"), row("c", 1, "new")],
        "false",
        true,
    );
    let result = m.guarded_merge(&ns, &req).await.unwrap();
    assert_eq!((result.inserted, result.updated, result.skipped), (1, 0, 1));
    assert_eq!(result.affected_ids, vec![RowId::String("c".into())]);
    assert_eq!(read(&m, &ns).await[0]["summary"], json!("original"));
    let result = m
        .guarded_merge(
            &ns,
            &merge(
                vec![row("a", 2, "fresh")],
                "target.epoch = source.epoch AND target.version < source.version",
                false,
            ),
        )
        .await
        .unwrap();
    assert_eq!((result.inserted, result.updated, result.skipped), (0, 1, 0));
    let result = m
        .guarded_merge(
            &ns,
            &merge(
                vec![row("a", 1, "stale"), row("absent", 1, "no insert")],
                "target.version < source.version",
                false,
            ),
        )
        .await
        .unwrap();
    assert_eq!((result.inserted, result.updated, result.skipped), (0, 0, 2));
    assert_eq!(read(&m, &ns).await[0]["summary"], json!("fresh"));
}

#[tokio::test]
async fn patch_vector_copy_is_bit_exact_and_unsupported_clear_has_no_effects() {
    let (_dir, m, ns) = fixture().await;
    let vector = vec![-0.0_f32, f32::from_bits(1), 0.3_f32];
    m.patch(
        &ns,
        &patch(
            "a",
            json!({"vector":vector,"pending":false}),
            Some("version = 1"),
        ),
    )
    .await
    .unwrap();
    let result = m
        .query(
            &ns,
            vec![1., 0., 0.],
            None,
            10,
            None,
            None,
            Some("id = 'a'".into()),
            true,
        )
        .await
        .unwrap();
    let actual = result.results[0].vector.as_ref().unwrap();
    assert_eq!(
        actual.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
        vector.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
    );
    assert_eq!(read(&m, &ns).await[0]["summary"], json!("original"));
    let before = read(&m, &ns).await;
    assert!(matches!(
        m.patch(
            &ns,
            &patch(
                "a",
                json!({"vector":null,"pending":true}),
                Some("version = 1"),
            ),
        )
        .await,
        Err(HevSearchError::Unsupported(_))
    ));
    assert_eq!(read(&m, &ns).await, before);
}

#[tokio::test]
async fn invalid_patch_and_guard_have_no_data_or_schema_effects() {
    let (_dir, m, ns) = fixture().await;
    let before = read(&m, &ns).await;
    for values in [
        json!({"new_column":"no"}),
        json!({"tags":7}),
        json!({"version":"wrong"}),
        json!({"id":"replacement"}),
        json!({"vector":[1.0]}),
    ] {
        assert!(m.patch(&ns, &patch("a", values, None)).await.is_err());
        assert_eq!(read(&m, &ns).await, before);
    }
    for condition in [
        "version =",
        "random() > 0.1",
        "version IN (SELECT version FROM rows)",
        "1",
        "true; DELETE FROM rows",
    ] {
        assert!(
            m.patch(&ns, &patch("a", json!({"summary":"bad"}), Some(condition)))
                .await
                .is_err(),
            "{condition}"
        );
        assert_eq!(read(&m, &ns).await, before);
    }
    let mut bad = row("a", 2, "bad");
    bad.attributes.insert("new_column".into(), json!("reject"));
    assert!(m
        .guarded_merge(&ns, &merge(vec![bad], "true", true))
        .await
        .is_err());
    assert_eq!(read(&m, &ns).await, before);
}

#[tokio::test]
async fn concurrent_conditional_patches_have_one_winner() {
    let (_dir, m, ns) = fixture().await;
    let left = patch(
        "a",
        json!({"summary":"left","version":2}),
        Some("version = 1"),
    );
    let right = patch(
        "a",
        json!({"summary":"right","version":2}),
        Some("version = 1"),
    );
    let (a, b) = tokio::join!(m.patch(&ns, &left), m.patch(&ns, &right));
    let mut applied = 0;
    for result in [a, b] {
        match result {
            Ok(r) => applied += r.patched,
            Err(HevSearchError::MutationConflict(_)) => {}
            other => panic!("unexpected result {other:?}"),
        }
    }
    assert_eq!(applied, 1);
    let rows = read(&m, &ns).await;
    assert_eq!(rows[0]["version"], json!(2));
    assert!([json!("left"), json!("right")].contains(&rows[0]["summary"]));
}

#[tokio::test]
async fn concurrent_insert_only_has_one_effect_and_no_duplicate() {
    let (_dir, m, ns) = fixture().await;
    let left = merge(vec![row("new", 1, "left")], "false", true);
    let right = merge(vec![row("new", 1, "right")], "false", true);
    let (a, b) = tokio::join!(m.guarded_merge(&ns, &left), m.guarded_merge(&ns, &right));
    let mut inserted = 0;
    for result in [a, b] {
        match result {
            Ok(r) => inserted += r.inserted,
            Err(HevSearchError::MutationConflict(_)) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(inserted, 1);
    assert_eq!(
        read(&m, &ns)
            .await
            .iter()
            .filter(|r| r["id"] == "new")
            .count(),
        1
    );
}

#[tokio::test]
async fn separate_managers_refresh_guards_and_preserve_raw_null_and_lists() {
    let (dir, m, ns) = fixture().await;
    let second = NamespaceManager::new(
        StorageRoot::local(dir.path()).unwrap(),
        HashMap::new(),
        test_metrics(),
    );
    let _ = read(&second, &ns).await;
    m.patch(
        &ns,
        &patch(
            "a",
            json!({"version":2,"tags":["legacy",null],"summary":null}),
            Some("version=1"),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        second
            .patch(
                &ns,
                &patch("a", json!({"summary":"stale"}), Some("version=1"))
            )
            .await
            .unwrap()
            .patched,
        0
    );
    assert_eq!(
        second
            .patch(
                &ns,
                &patch(
                    "a",
                    json!({"summary":"fresh"}),
                    Some("version=2 AND summary IS NULL AND tags = make_array('legacy',NULL)")
                )
            )
            .await
            .unwrap()
            .patched,
        1
    );
    assert_eq!(read(&second, &ns).await[0]["summary"], "fresh");
}

#[tokio::test]
async fn heterogeneous_sets_duplicates_and_limits_refuse_before_effects() {
    let (_dir, m, ns) = fixture().await;
    let before = read(&m, &ns).await;
    let a = patch("a", json!({"summary":"x"}), None).rows.remove(0);
    let b = patch("b", json!({"version":3}), None).rows.remove(0);
    for rows in [
        vec![a.clone(), b],
        vec![a.clone(), a.clone()],
        vec![a; 101],
        vec![],
    ] {
        assert!(m
            .patch(
                &ns,
                &PatchRequest {
                    rows,
                    condition: None,
                    timeout_ms: 5000
                }
            )
            .await
            .is_err());
        assert_eq!(read(&m, &ns).await, before);
    }
}

#[tokio::test]
async fn text_changes_feed_phrase_and_null_attributes_stay_null() {
    let (_dir, m, ns) = fixture().await;
    m.patch(
        &ns,
        &patch(
            "a",
            json!({"text":"New Café, FINISH","summary":null}),
            Some("version=1"),
        ),
    )
    .await
    .unwrap();
    let r=m.aggregate(&AggregateRequest{tables:BTreeMap::from([("rows".into(),ns.to_string())]),versions:BTreeMap::new(),sql:"SELECT id, summary FROM rows WHERE phrase_matches(text, 'new café, finish') ORDER BY id".into(),max_rows:10,max_bytes:10000,timeout_ms:5000,explain_only:false}).await.unwrap();
    assert_eq!(
        r.rows,
        json!([{"id":"a","summary":null}])
            .as_array()
            .unwrap()
            .clone()
    );
}

#[tokio::test]
async fn multivector_patch_preserves_other_fields() {
    let dir = tempfile::tempdir().unwrap();
    let m = NamespaceManager::new(
        StorageRoot::local(dir.path()).unwrap(),
        HashMap::new(),
        test_metrics(),
    );
    let ns = NamespaceId::new("multi-patch").unwrap();
    let mut initial = row("a", 1, "keep");
    initial.vector.clear();
    initial.vectors = Some(vec![vec![1., 0., 0.], vec![0., 1., 0.]]);
    m.upsert(&ns, vec![initial]).await.unwrap();
    m.patch(
        &ns,
        &patch("a", json!({"vectors":[[0.25,0.5,0.75]]}), Some("version=1")),
    )
    .await
    .unwrap();
    let rows = read(&m, &ns).await;
    assert_eq!(rows[0]["vector"], json!([[0.25, 0.5, 0.75]]));
    assert_eq!(rows[0]["summary"], "keep");
}

#[tokio::test]
async fn metadata_only_guarded_postimage_remains_readable() {
    let (_dir, m, ns) = fixture().await;
    let mut postimage = row("a", 2, "metadata");
    postimage.vector.clear();
    m.guarded_merge(
        &ns,
        &merge(vec![postimage], "target.version < source.version", false),
    )
    .await
    .unwrap();
    let rows = read(&m, &ns).await;
    assert!(rows[0]["vector"].is_null());
    assert_eq!(rows[0]["version"], 2);
    m.patch(
        &ns,
        &patch(
            "a",
            json!({"summary":"metadata patched"}),
            Some("version=2"),
        ),
    )
    .await
    .unwrap();
    let patched = read(&m, &ns).await;
    assert!(patched[0]["vector"].is_null());
    assert_eq!(patched[0]["summary"], "metadata patched");
    let hidden = m
        .query(
            &ns,
            vec![1., 0., 0.],
            None,
            10,
            None,
            None,
            Some("id = 'a'".into()),
            true,
        )
        .await
        .unwrap();
    assert!(
        hidden.results.is_empty(),
        "metadata postimage must exclude old vector from ANN"
    );
    let completion = patch(
        "a",
        json!({"vector":[0.25,0.5,0.75],"pending":false}),
        Some("version=2 AND epoch='one'"),
    );
    assert_eq!(m.patch(&ns, &completion).await.unwrap().patched, 1);
    let completed = read(&m, &ns).await;
    assert_eq!(completed[0]["vector"], json!([0.25, 0.5, 0.75]));
    assert_eq!(completed[0]["summary"], "metadata patched");
    assert_eq!(
        m.patch(
            &ns,
            &patch(
                "a",
                json!({"vector":[9.,9.,9.],"pending":false}),
                Some("version=1")
            )
        )
        .await
        .unwrap()
        .patched,
        0
    );
    assert_eq!(read(&m, &ns).await, completed);
}

#[tokio::test]
async fn unsigned_ids_keep_full_width_in_guards_and_effects() {
    let dir = tempfile::tempdir().unwrap();
    let m = NamespaceManager::new(
        StorageRoot::local(dir.path()).unwrap(),
        HashMap::new(),
        test_metrics(),
    );
    let ns = NamespaceId::new("unsigned-guard").unwrap();
    let id = RowId::U64(u64::MAX);
    let mut initial = row("unused", 1, "before");
    initial.id = id.clone();
    m.upsert(&ns, vec![initial]).await.unwrap();
    let mut req = patch("unused", json!({"summary":"after"}), Some("version=1"));
    req.rows[0].id = id.clone();
    let result = m.patch(&ns, &req).await.unwrap();
    assert_eq!(result.affected_ids, vec![id]);
    assert_eq!(read(&m, &ns).await[0]["summary"], "after");
}
