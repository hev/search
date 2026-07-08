//! Embedded-mode integration test: `NamespaceManager` against a local
//! filesystem `StorageRoot` (no S3, no MinIO, no network).
//!
//! Proves the `Scheme::Local` path end-to-end: `lancedb::connect`
//! opens a local Lance table from the `file://` URI, an upsert writes
//! a directory tree under the base dir, a query reads it back, and the
//! delete path drops through `object_store::local::LocalFileSystem` to
//! remove the namespace's objects. Unlike the cloud manager tests this
//! one is deliberately **not** `#[ignore]`d — embedded mode is exactly
//! the zero-infrastructure case, so it runs in CI and locally as-is.

use std::collections::HashMap;

use hevsearch_core::metrics::test_metrics;
use hevsearch_core::{
    DistanceMetric, FuzzyMaxEditDistance, FuzzyRequest, HevSearchError, ListOrder, NamespaceId,
    NamespaceManager, RowId, RowIdType, StorageRoot, UpsertRow,
};
use serde_json::json;
use tempfile::TempDir;

const DIM: usize = 8;

fn unit_vector(axis: usize) -> Vec<f32> {
    let mut v = vec![0.0_f32; DIM];
    v[axis] = 1.0;
    v
}

fn local_manager(dir: &TempDir) -> NamespaceManager {
    NamespaceManager::new(
        StorageRoot::local(dir.path()).unwrap(),
        HashMap::new(),
        test_metrics(),
    )
}

#[tokio::test]
async fn local_fs_upsert_query_roundtrip() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-roundtrip").unwrap();

    let rows: Vec<UpsertRow> = vec![
        (1u64, unit_vector(0)).into(),
        (2u64, unit_vector(1)).into(),
        (3u64, unit_vector(2)).into(),
    ];
    manager.upsert(&ns, rows).await.expect("local upsert");

    // The namespace directory must now exist on the local filesystem
    // under the base dir — proof that lancedb opened and wrote to the
    // `file://` root rather than silently failing or going elsewhere.
    assert!(
        dir.path().join("embedded-roundtrip").is_dir(),
        "expected a local Lance table directory for the namespace"
    );

    let results = manager
        .query(&ns, unit_vector(0), None, 3, None, None, None, true)
        .await
        .expect("local query");

    assert_eq!(results.results.len(), 3, "should return top-3 hits");
    let top = &results.results[0];
    assert_eq!(top.id, 1, "nearest neighbour of axis-0 must be id=1");
    assert!(
        top.score < 0.01,
        "self-distance should be ~0, got {}",
        top.score
    );
    let top_vector = top
        .vector
        .as_ref()
        .expect("default query must return the stored vector");
    assert_eq!(top_vector.len(), DIM, "returned vector width must match");
}

#[tokio::test]
async fn local_fs_distance_metric_controls_single_vector_ranking() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns_l2 = NamespaceId::new("embedded-metric-l2").unwrap();
    let ns_cosine = NamespaceId::new("embedded-metric-cosine").unwrap();

    let rows = || {
        vec![
            UpsertRow {
                id: RowId::U64(1),
                vector: vec![10.0, 0.0],
                vectors: None,
                text: None,
                attributes: serde_json::Map::new(),
            },
            UpsertRow {
                id: RowId::U64(2),
                vector: vec![0.9, 0.1],
                vectors: None,
                text: None,
                attributes: serde_json::Map::new(),
            },
        ]
    };

    manager
        .upsert_with_distance_metric(&ns_l2, rows(), Some(DistanceMetric::L2))
        .await
        .expect("l2 upsert");
    manager
        .upsert_with_distance_metric(&ns_cosine, rows(), Some(DistanceMetric::Cosine))
        .await
        .expect("cosine upsert");

    let l2 = manager
        .query(&ns_l2, vec![1.0, 0.0], None, 1, None, None, None, false)
        .await
        .expect("l2 query");
    let cosine = manager
        .query(&ns_cosine, vec![1.0, 0.0], None, 1, None, None, None, false)
        .await
        .expect("cosine query");

    assert_eq!(l2.results[0].id, RowId::U64(2));
    assert_eq!(cosine.results[0].id, RowId::U64(1));
    assert_eq!(
        manager
            .info(&ns_cosine)
            .await
            .expect("info")
            .expect("namespace")
            .distance_metric,
        DistanceMetric::Cosine
    );
}

#[tokio::test]
async fn local_fs_distance_metric_is_fixed_and_multivector_is_cosine_only() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-metric-fixed").unwrap();

    manager
        .upsert_with_distance_metric(
            &ns,
            vec![(1u64, vec![1.0, 0.0]).into()],
            Some(DistanceMetric::Dot),
        )
        .await
        .expect("dot upsert");

    let err = manager
        .upsert_with_distance_metric(
            &ns,
            vec![(2u64, vec![0.0, 1.0]).into()],
            Some(DistanceMetric::Cosine),
        )
        .await
        .expect_err("metric changes must be rejected");
    assert!(matches!(err, HevSearchError::InvalidRequest(_)));

    let mv_ns = NamespaceId::new("embedded-metric-mv").unwrap();
    let err = manager
        .upsert_with_distance_metric(
            &mv_ns,
            vec![UpsertRow {
                id: RowId::U64(1),
                vector: Vec::new(),
                vectors: Some(vec![vec![1.0, 0.0]]),
                text: None,
                attributes: serde_json::Map::new(),
            }],
            Some(DistanceMetric::L2),
        )
        .await
        .expect_err("multivector non-cosine metric must be rejected");
    assert!(matches!(err, HevSearchError::InvalidRequest(_)));
}

#[tokio::test]
async fn local_fs_string_ids_roundtrip_query_list_facet_and_info() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-string-ids").unwrap();

    let attr = |section: &str, route: &str| {
        let mut attributes = serde_json::Map::new();
        attributes.insert("section".into(), json!(section));
        attributes.insert("route".into(), json!(route));
        attributes
    };

    manager
        .upsert(
            &ns,
            vec![
                UpsertRow {
                    id: RowId::String("set-a#warnings#0".into()),
                    vector: unit_vector(0),
                    vectors: None,
                    text: Some("boxed warning".into()),
                    attributes: attr("warnings", "oral"),
                },
                UpsertRow {
                    id: RowId::String("set-a#dosage#1".into()),
                    vector: unit_vector(1),
                    vectors: None,
                    text: Some("dosage instruction".into()),
                    attributes: attr("dosage", "iv"),
                },
                UpsertRow {
                    id: RowId::String("set-b#warnings#2".into()),
                    vector: unit_vector(2),
                    vectors: None,
                    text: Some("warning detail".into()),
                    attributes: attr("warnings", "oral"),
                },
            ],
        )
        .await
        .expect("string-id upsert");

    let info = manager.info(&ns).await.expect("info").expect("namespace");
    assert_eq!(info.id_type, RowIdType::String);
    assert_eq!(info.row_count, 3);

    let results = manager
        .query(
            &ns,
            unit_vector(0),
            None,
            3,
            None,
            None,
            Some("section = 'warnings'".into()),
            false,
        )
        .await
        .expect("filtered query");
    assert_eq!(
        results.results[0].id,
        RowId::String("set-a#warnings#0".into())
    );
    assert!(results
        .results
        .iter()
        .all(|r| r.attributes.get("section") == Some(&json!("warnings"))));

    let list = manager
        .list(&ns, 2, ListOrder::Desc, None, None)
        .await
        .expect("list first page");
    assert_eq!(list.rows.len(), 2);
    let cursor = list.next_cursor.expect("cursor for third row");
    let next = manager
        .list(
            &ns,
            2,
            ListOrder::Desc,
            Some(hevsearch_core::decode_list_cursor(&cursor).expect("decode cursor")),
            None,
        )
        .await
        .expect("list second page");
    assert_eq!(next.rows.len(), 1);

    let facet = manager
        .facet(&ns, Some("route = 'oral'".into()), &["section".into()], 10)
        .await
        .expect("facet");
    assert_eq!(facet.facets[0].buckets[0].value, json!("warnings"));
    assert_eq!(facet.facets[0].buckets[0].count, 2);

    let err = manager
        .upsert(
            &ns,
            vec![UpsertRow {
                id: RowId::U64(4),
                vector: unit_vector(3),
                vectors: None,
                text: None,
                attributes: serde_json::Map::new(),
            }],
        )
        .await
        .expect_err("numeric id in string-id namespace must fail");
    assert!(format!("{err}").contains("namespace id_type is string"));
}

#[tokio::test]
async fn local_fs_delete_ids_and_filter_remove_rows() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-row-delete").unwrap();

    let mut oral = serde_json::Map::new();
    oral.insert("route".into(), json!("oral"));
    let mut iv = serde_json::Map::new();
    iv.insert("route".into(), json!("iv"));

    manager
        .upsert(
            &ns,
            vec![
                UpsertRow {
                    id: RowId::U64(1),
                    vector: unit_vector(0),
                    vectors: None,
                    text: Some("alpha".into()),
                    attributes: oral.clone(),
                },
                UpsertRow {
                    id: RowId::U64(2),
                    vector: unit_vector(1),
                    vectors: None,
                    text: Some("beta".into()),
                    attributes: oral,
                },
                UpsertRow {
                    id: RowId::U64(3),
                    vector: unit_vector(2),
                    vectors: None,
                    text: Some("gamma".into()),
                    attributes: iv,
                },
            ],
        )
        .await
        .expect("upsert");

    let deleted = manager
        .delete_ids(&ns, &[RowId::U64(2)])
        .await
        .expect("delete id");
    assert_eq!(deleted, 1);
    let info = manager.info(&ns).await.expect("info").expect("namespace");
    assert_eq!(info.row_count, 2);
    let page = manager
        .list(&ns, 10, ListOrder::Asc, None, None)
        .await
        .expect("list after id delete");
    let ids: Vec<RowId> = page.rows.into_iter().map(|r| r.id).collect();
    assert_eq!(ids, vec![RowId::U64(1), RowId::U64(3)]);

    let deleted = manager
        .delete_rows(&ns, "route = 'oral'")
        .await
        .expect("delete filter");
    assert_eq!(deleted, 1);
    let page = manager
        .list(&ns, 10, ListOrder::Asc, None, None)
        .await
        .expect("list after filter delete");
    let ids: Vec<RowId> = page.rows.into_iter().map(|r| r.id).collect();
    assert_eq!(ids, vec![RowId::U64(3)]);
}

#[tokio::test]
async fn local_fs_list_filter_paginates_matching_subset() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-list-filter").unwrap();

    let attr = |keep: bool| {
        let mut attributes = serde_json::Map::new();
        attributes.insert("keep".into(), json!(keep));
        attributes
    };

    manager
        .upsert(
            &ns,
            vec![
                UpsertRow {
                    id: RowId::U64(1),
                    vector: unit_vector(0),
                    vectors: None,
                    text: None,
                    attributes: attr(true),
                },
                UpsertRow {
                    id: RowId::U64(2),
                    vector: unit_vector(1),
                    vectors: None,
                    text: None,
                    attributes: attr(false),
                },
                UpsertRow {
                    id: RowId::U64(3),
                    vector: unit_vector(2),
                    vectors: None,
                    text: None,
                    attributes: attr(true),
                },
            ],
        )
        .await
        .expect("upsert");

    let first = manager
        .list(&ns, 1, ListOrder::Asc, None, Some("keep = true".into()))
        .await
        .expect("filtered list first page");
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].id, RowId::U64(1));
    let cursor = first.next_cursor.expect("cursor for second kept row");

    let second = manager
        .list(
            &ns,
            1,
            ListOrder::Asc,
            Some(hevsearch_core::decode_list_cursor(&cursor).expect("decode cursor")),
            Some("keep = true".into()),
        )
        .await
        .expect("filtered list second page");
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0].id, RowId::U64(3));
    assert!(second.next_cursor.is_none());
}

#[tokio::test]
async fn local_fs_delete_string_ids_quotes_literals_and_rejects_wrong_type() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-row-delete-string").unwrap();

    manager
        .upsert(
            &ns,
            vec![
                UpsertRow {
                    id: RowId::String("set-a#warnings#0".into()),
                    vector: unit_vector(0),
                    vectors: None,
                    text: None,
                    attributes: serde_json::Map::new(),
                },
                UpsertRow {
                    id: RowId::String("set-b'special#dosage#1".into()),
                    vector: unit_vector(1),
                    vectors: None,
                    text: None,
                    attributes: serde_json::Map::new(),
                },
            ],
        )
        .await
        .expect("upsert string ids");

    let deleted = manager
        .delete_ids(&ns, &[RowId::String("set-b'special#dosage#1".into())])
        .await
        .expect("delete quoted string id");
    assert_eq!(deleted, 1);
    let page = manager
        .list(&ns, 10, ListOrder::Asc, None, None)
        .await
        .expect("list after string id delete");
    assert_eq!(page.rows[0].id, RowId::String("set-a#warnings#0".into()));

    let err = manager
        .delete_ids(&ns, &[RowId::U64(1)])
        .await
        .expect_err("numeric delete id in string namespace must fail");
    assert!(format!("{err}").contains("namespace id_type is string"));
}

#[tokio::test]
async fn local_fs_fts_text_search() {
    // The path the Python binding's text/hybrid search relies on:
    // build a BM25 index over the text column, then run an FTS-only
    // query. Rows carry a vector (hevsearch is a vector engine) plus text.
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-fts").unwrap();

    let rows: Vec<UpsertRow> = vec![
        UpsertRow {
            id: hevsearch_core::RowId::U64(1),
            vector: unit_vector(0),
            vectors: None,
            text: Some("the quick brown fox".into()),
            attributes: serde_json::Map::new(),
        },
        UpsertRow {
            id: hevsearch_core::RowId::U64(2),
            vector: unit_vector(1),
            vectors: None,
            text: Some("a lazy dog sleeps".into()),
            attributes: serde_json::Map::new(),
        },
        UpsertRow {
            id: hevsearch_core::RowId::U64(3),
            vector: unit_vector(2),
            vectors: None,
            text: Some("the fox runs fast".into()),
            attributes: serde_json::Map::new(),
        },
    ];
    manager.upsert(&ns, rows).await.expect("local upsert");
    manager
        .create_fts_index(&ns)
        .await
        .expect("create fts index");

    // FTS-only: empty vector, text set.
    let results = manager
        .query(
            &ns,
            Vec::new(),
            None,
            10,
            None,
            Some("fox".into()),
            None,
            false,
        )
        .await
        .expect("fts query");
    let ids: Vec<RowId> = results.results.iter().map(|r| r.id.clone()).collect();
    assert!(
        ids.contains(&RowId::U64(1)) && ids.contains(&RowId::U64(3)),
        "'fox' should match rows 1 and 3, got {ids:?}"
    );
    assert!(
        !ids.contains(&RowId::U64(2)),
        "row 2 has no 'fox' and must not match, got {ids:?}"
    );
}

#[tokio::test]
async fn local_fs_fuzzy_fts_matches_typo_query() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-fuzzy-fts").unwrap();

    manager
        .upsert(
            &ns,
            vec![
                UpsertRow {
                    id: RowId::U64(1),
                    vector: unit_vector(0),
                    vectors: None,
                    text: Some("kubernetes connection timeout".into()),
                    attributes: serde_json::Map::new(),
                },
                UpsertRow {
                    id: RowId::U64(2),
                    vector: unit_vector(1),
                    vectors: None,
                    text: Some("ordinary billing report".into()),
                    attributes: serde_json::Map::new(),
                },
            ],
        )
        .await
        .expect("local upsert");
    manager
        .create_fts_index(&ns)
        .await
        .expect("create fts index");

    let exact = manager
        .query(
            &ns,
            Vec::new(),
            None,
            10,
            None,
            Some("kubernets".into()),
            None,
            false,
        )
        .await
        .expect("exact typo query");
    assert!(
        exact.results.is_empty(),
        "exact FTS should not match the typo"
    );

    let fuzzy = manager
        .query_with_fuzzy(
            &ns,
            Vec::new(),
            None,
            10,
            None,
            // RFC 0001 moved FTS to unstemmed alyze `word_v4` tokens, and
            // RFC 0004's fuzzy ladder keeps 1-5 char tokens exact. Use the
            // longer RFC 0004 probe so a fixed edit distance still exercises
            // genuine typo tolerance instead of the intentional short-token
            // exact path.
            Some("kubernets".into()),
            Some(FuzzyRequest {
                max_edit_distance: FuzzyMaxEditDistance::Fixed(1),
            }),
            None,
            false,
        )
        .await
        .expect("fuzzy typo query");
    assert_eq!(fuzzy.results[0].id, RowId::U64(1));
}

#[tokio::test]
async fn local_fs_query_filter_narrows_vector_results() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-filter-vector").unwrap();

    let rows: Vec<UpsertRow> = vec![
        (1u64, unit_vector(0)).into(),
        (2u64, unit_vector(1)).into(),
        (3u64, unit_vector(2)).into(),
    ];
    manager.upsert(&ns, rows).await.expect("local upsert");

    let results = manager
        .query(
            &ns,
            unit_vector(0),
            None,
            3,
            None,
            None,
            Some("id > 1".into()),
            false,
        )
        .await
        .expect("filtered vector query");
    let mut ids: Vec<RowId> = results.results.iter().map(|r| r.id.clone()).collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![RowId::U64(2), RowId::U64(3)],
        "filter should exclude id=1"
    );
}

#[tokio::test]
async fn local_fs_query_filter_accepts_ingested_at_ranges() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-filter-ingested").unwrap();

    let rows: Vec<UpsertRow> = vec![(1u64, unit_vector(0)).into(), (2u64, unit_vector(1)).into()];
    manager.upsert(&ns, rows).await.expect("local upsert");

    let all = manager
        .query(&ns, unit_vector(0), None, 2, None, None, None, false)
        .await
        .expect("unfiltered query");
    let cutoff = all.results[0]
        .ingested_at_micros
        .expect("ingested_at on query hit");

    let results = manager
        .query(
            &ns,
            unit_vector(0),
            None,
            2,
            None,
            None,
            Some(format!("_ingested_at >= to_timestamp_micros({cutoff})")),
            false,
        )
        .await
        .expect("filtered ingested_at query");
    assert_eq!(results.results.len(), 2);
}

#[tokio::test]
async fn local_fs_query_filter_narrows_fts_and_hybrid_results() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-filter-fts-hybrid").unwrap();

    let rows: Vec<UpsertRow> = vec![
        UpsertRow {
            id: hevsearch_core::RowId::U64(1),
            vector: unit_vector(0),
            vectors: None,
            text: Some("fox warning".into()),
            attributes: serde_json::Map::new(),
        },
        UpsertRow {
            id: hevsearch_core::RowId::U64(2),
            vector: unit_vector(1),
            vectors: None,
            text: Some("fox dosing".into()),
            attributes: serde_json::Map::new(),
        },
        UpsertRow {
            id: hevsearch_core::RowId::U64(3),
            vector: unit_vector(2),
            vectors: None,
            text: Some("dog warning".into()),
            attributes: serde_json::Map::new(),
        },
    ];
    manager.upsert(&ns, rows).await.expect("local upsert");
    manager
        .create_fts_index(&ns)
        .await
        .expect("create fts index");

    let fts = manager
        .query(
            &ns,
            Vec::new(),
            None,
            10,
            None,
            Some("fox".into()),
            Some("id = 2".into()),
            false,
        )
        .await
        .expect("filtered fts query");
    let fts_ids: Vec<RowId> = fts.results.iter().map(|r| r.id.clone()).collect();
    assert_eq!(fts_ids, vec![RowId::U64(2)]);

    let hybrid = manager
        .query(
            &ns,
            unit_vector(0),
            None,
            10,
            None,
            Some("fox".into()),
            Some("id = 2".into()),
            false,
        )
        .await
        .expect("filtered hybrid query");
    let hybrid_ids: Vec<RowId> = hybrid.results.iter().map(|r| r.id.clone()).collect();
    assert_eq!(hybrid_ids, vec![RowId::U64(2)]);
}

#[tokio::test]
async fn local_fs_facet_counts_attributes_with_filter_null_and_truncation() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-facet").unwrap();

    let attr = |section: serde_json::Value, route: Option<&str>| {
        let mut attributes = serde_json::Map::new();
        attributes.insert("section".into(), section);
        if let Some(route) = route {
            attributes.insert("route".into(), json!(route));
        }
        attributes
    };
    manager
        .upsert(
            &ns,
            vec![
                UpsertRow {
                    id: hevsearch_core::RowId::U64(1),
                    vector: unit_vector(0),
                    vectors: None,
                    text: Some("warning oral".into()),
                    attributes: attr(json!("warnings"), Some("oral")),
                },
                UpsertRow {
                    id: hevsearch_core::RowId::U64(2),
                    vector: unit_vector(1),
                    vectors: None,
                    text: Some("dosage oral".into()),
                    attributes: attr(json!("dosage"), Some("oral")),
                },
                UpsertRow {
                    id: hevsearch_core::RowId::U64(3),
                    vector: unit_vector(2),
                    vectors: None,
                    text: Some("warning missing".into()),
                    attributes: attr(json!("warnings"), None),
                },
            ],
        )
        .await
        .expect("upsert");

    let result = manager
        .facet(
            &ns,
            Some("id >= 1".into()),
            &["section".into(), "route".into()],
            1,
        )
        .await
        .expect("facet");

    let section = result.facets.iter().find(|f| f.field == "section").unwrap();
    assert!(section.truncated);
    assert_eq!(section.buckets[0].value, json!("warnings"));
    assert_eq!(section.buckets[0].count, 2);

    let route = result.facets.iter().find(|f| f.field == "route").unwrap();
    assert!(route.truncated);
    assert_eq!(route.buckets[0].value, json!("oral"));
    assert_eq!(route.buckets[0].count, 2);

    let narrowed = manager
        .facet(
            &ns,
            Some("section = 'dosage'".into()),
            &["section".into()],
            10,
        )
        .await
        .expect("filtered facet");
    assert_eq!(narrowed.facets[0].buckets.len(), 1);
    assert_eq!(narrowed.facets[0].buckets[0].value, json!("dosage"));
    assert_eq!(narrowed.facets[0].buckets[0].count, 1);

    manager
        .create_scalar_index(&ns, "section")
        .await
        .expect("attribute scalar index");
}

#[tokio::test]
async fn local_fs_facet_rejects_bad_filter_and_non_facetable_field() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-facet-errors").unwrap();
    let mut attributes = serde_json::Map::new();
    attributes.insert("section".into(), json!("warnings"));
    manager
        .upsert(
            &ns,
            vec![UpsertRow {
                id: hevsearch_core::RowId::U64(1),
                vector: unit_vector(0),
                vectors: None,
                text: Some("warning".into()),
                attributes,
            }],
        )
        .await
        .expect("upsert");

    let err = manager
        .facet(&ns, Some("section =".into()), &["section".into()], 10)
        .await
        .expect_err("malformed facet filter must fail");
    assert!(matches!(err, HevSearchError::InvalidRequest(_)));

    let err = manager
        .facet(&ns, None, &["vector".into()], 10)
        .await
        .expect_err("vector is not facetable");
    assert!(matches!(err, HevSearchError::InvalidRequest(_)));
}

#[tokio::test]
async fn local_fs_facet_empty_namespace_returns_empty() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-facet-empty").unwrap();

    let result = manager
        .facet(&ns, None, &["id".into()], 10)
        .await
        .expect("empty facet");
    assert!(result.facets.is_empty());
}

#[tokio::test]
async fn local_fs_query_filter_zero_match_and_malformed_predicate() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-filter-errors").unwrap();

    let rows: Vec<UpsertRow> = vec![(1u64, unit_vector(0)).into(), (2u64, unit_vector(1)).into()];
    manager.upsert(&ns, rows).await.expect("local upsert");

    let empty = manager
        .query(
            &ns,
            unit_vector(0),
            None,
            10,
            None,
            None,
            Some("id > 99".into()),
            false,
        )
        .await
        .expect("zero-match filtered query");
    assert!(empty.results.is_empty());

    let err = manager
        .query(
            &ns,
            unit_vector(0),
            None,
            10,
            None,
            None,
            Some("id =".into()),
            false,
        )
        .await
        .expect_err("malformed filter should fail");
    match err {
        HevSearchError::InvalidRequest(msg) => assert!(msg.contains("filter"), "{msg}"),
        other => panic!("expected InvalidRequest, got {other:?}"),
    }
}

#[tokio::test]
async fn local_fs_delete_removes_namespace_objects() {
    let dir = TempDir::new().unwrap();
    let manager = local_manager(&dir);
    let ns = NamespaceId::new("embedded-delete").unwrap();

    let rows: Vec<UpsertRow> = vec![(1u64, unit_vector(0)).into()];
    manager.upsert(&ns, rows).await.expect("local upsert");
    assert!(dir.path().join("embedded-delete").is_dir());

    // Delete drops into the local object store, lists the namespace
    // prefix, and removes each object. At least the manifest + data
    // files should come back in the count.
    let deleted = manager.delete(&ns).await.expect("local delete");
    assert!(
        deleted > 0,
        "delete should remove at least one object, got {deleted}"
    );
}
