//! Bounded mutations executed as native Lance transactions. Profiles are declared
//! separately; no schema changes, read/merge/upsert patch emulation, or replay loop.

use super::*;
use datafusion::{
    common::{
        tree_node::{TreeNode, TreeNodeRecursion},
        DFSchema,
    },
    datasource::MemTable,
    logical_expr::{Expr, ExprSchemable, Volatility},
    prelude::SessionContext,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{future::Future, sync::atomic::AtomicBool, time::Duration};

const MAX_ROWS: usize = 100;
const MAX_BYTES: usize = 1024 * 1024;
fn default_timeout() -> u64 {
    5000
}

/// Assignments for one typed row. Omitted fields are preserved.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchRow {
    /// Existing row identity.
    pub id: RowId,
    /// Attribute assignments, `text`, or the namespace's vector payload field.
    pub set: BTreeMap<String, Value>,
}

/// One native atomic update of at most 100 selected IDs and 1 MiB input.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchRequest {
    /// Unique typed row IDs and literal assignments.
    pub rows: Vec<PatchRow>,
    /// Optional deterministic SQL predicate over current values (NULL-safe SQL semantics).
    #[serde(default)]
    pub condition: Option<String>,
    /// Deadline over all phases, 1..=30000 milliseconds; post-dispatch expiry is unknown.
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}

/// Committed patch effect at one exact Lance version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatchResult {
    /// Number of matched and updated rows, including assignments of equal values.
    pub patched: u64,
    /// Exact affected typed IDs; all selected from the same transaction snapshot.
    pub affected_ids: Vec<RowId>,
    /// Data commit version, or the read version for a no-op.
    pub version: u64,
}

/// Full postimages guarded atomically by current/incoming row values.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuardedMergeRequest {
    /// Complete replacement rows, at most 100 and 1 MiB in total.
    pub rows: Vec<UpsertRow>,
    /// Required deterministic SQL over `target` (old) and `source` (incoming).
    /// Use `false` for insert-only semantics; this is never silently unconditional.
    pub condition: String,
    /// Explicit policy for absent rows. Unmatched rows do not bypass matched guards.
    pub insert_if_missing: bool,
    /// Deadline over all phases, 1..=30000 milliseconds.
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}

/// Native guarded merge counts and exact affected IDs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardedMergeResult {
    /// Newly inserted row count.
    pub inserted: u64,
    /// Existing rows replaced after matching the predicate.
    pub updated: u64,
    /// Requested rows left untouched.
    pub skipped: u64,
    /// Exact affected typed IDs.
    pub affected_ids: Vec<RowId>,
    /// Data commit version, or snapshot version for a no-op.
    pub version: u64,
}

fn invalid(message: impl ToString) -> HevSearchError {
    HevSearchError::InvalidRequest(message.to_string())
}
fn input_budget<T: Serialize>(
    request: &T,
    rows: usize,
    timeout: u64,
) -> Result<(), HevSearchError> {
    if !(1..=MAX_ROWS).contains(&rows) || !(1..=30_000).contains(&timeout) {
        return Err(invalid(
            "mutations require 1..=100 rows and timeout_ms in 1..=30000",
        ));
    }
    if serde_json::to_vec(request).map_err(invalid)?.len() > MAX_BYTES {
        return Err(invalid("mutation input exceeds 1 MiB"));
    }
    Ok(())
}
fn validate_ids<'a>(
    ids: impl Iterator<Item = &'a RowId>,
    ty: RowIdType,
) -> Result<Vec<RowId>, HevSearchError> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for id in ids {
        if id.id_type() != ty || !seen.insert(id.clone()) {
            return Err(invalid(
                "mutation IDs must have the namespace type and be unique",
            ));
        }
        result.push(id.clone());
    }
    Ok(result)
}
fn id_literal(id: &RowId) -> String {
    match id {
        RowId::U64(value) => format!("arrow_cast('{value}', 'UInt64')"),
        RowId::String(_) => id.to_sql_literal(),
    }
}
fn id_filter(ids: &[RowId]) -> String {
    format!(
        "id IN ({})",
        ids.iter().map(id_literal).collect::<Vec<_>>().join(",")
    )
}
/// Validate guards before starting a write. Immutable row expressions only: no
/// subqueries, aggregates, window functions, variables, or volatile/stable UDFs.
fn validate_condition(condition: &str, schema: &DFSchema) -> Result<(), HevSearchError> {
    if condition.is_empty() || condition.len() > 16 * 1024 {
        return Err(invalid("condition must contain 1..=16384 bytes"));
    }
    let expr = SessionContext::new()
        .state()
        .create_logical_expr(condition, schema)
        .map_err(invalid)?;
    if expr.get_type(schema).map_err(invalid)? != DataType::Boolean {
        return Err(invalid("condition must be boolean"));
    }
    expr.apply(|node| {
        match node {
            Expr::ScalarFunction(f)
                if f.func.signature().volatility != Volatility::Immutable
                    || !["make_array", "coalesce"].contains(&f.func.name()) =>
            {
                return Err(datafusion::error::DataFusionError::Plan(
                    "guard functions must be immutable supported row functions".into(),
                ))
            }
            Expr::Exists(_)
            | Expr::InSubquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::AggregateFunction(_)
            | Expr::WindowFunction(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Placeholder(_) => {
                return Err(datafusion::error::DataFusionError::Plan(
                    "guards must be deterministic row predicates".into(),
                ))
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(invalid)?;
    Ok(())
}

fn execute_error(error: lance::Error) -> HevSearchError {
    match error {
        lance::Error::CommitConflict { .. }
        | lance::Error::RetryableCommitConflict { .. }
        | lance::Error::TooMuchWriteContention { .. }
        | lance::Error::IncompatibleTransaction { .. } => {
            HevSearchError::MutationConflict(error.to_string())
        }
        _ => HevSearchError::MutationUnknown(error.to_string()),
    }
}
async fn deadline<T>(
    timeout: u64,
    started: &AtomicBool,
    work: impl Future<Output = Result<T, HevSearchError>>,
) -> Result<T, HevSearchError> {
    tokio::time::timeout(Duration::from_millis(timeout), work)
        .await
        .map_err(|_| {
            if started.load(Ordering::SeqCst) {
                HevSearchError::MutationUnknown(
                    "deadline after write dispatch; do not assume noncommit or replay".into(),
                )
            } else {
                invalid("deadline before write dispatch; no mutation effects")
            }
        })?
}

impl NamespaceManager {
    async fn mutation_snapshot(
        &self,
        ns: &NamespaceId,
    ) -> Result<(Arc<lance::Dataset>, NamespaceSchemaInfo, Arc<Schema>), HevSearchError> {
        let (table, _) = self
            .open_existing(ns)
            .await?
            .ok_or_else(|| invalid("namespace does not exist; declare its profile first"))?;
        let dataset = table
            .dataset()
            .ok_or_else(|| HevSearchError::Unsupported("mutations require native Lance".into()))?
            .get()
            .await
            .map_err(|e| HevSearchError::Backend(e.to_string()))?;
        // Refresh a request-owned Dataset, never switch the pooled Table into
        // time travel or evaluate guards against a stale cross-process cache.
        let mut latest = dataset.as_ref().clone();
        latest
            .checkout_latest()
            .await
            .map_err(|e| HevSearchError::Backend(e.to_string()))?;
        let dataset = Arc::new(latest);
        let schema = Arc::new(Schema::from(dataset.schema()));
        let info = read_schema_info_from_schema(&schema)?;
        Ok((dataset, info, schema))
    }

    /// Atomically patch literal fields on selected IDs without replacing other data.
    /// Conflicts are rejected without automatic snapshot retries; uncertain execution
    /// errors are explicitly unknown and are never replayed by the engine.
    pub async fn patch(
        &self,
        ns: &NamespaceId,
        request: &PatchRequest,
    ) -> Result<PatchResult, HevSearchError> {
        input_budget(request, request.rows.len(), request.timeout_ms)?;
        let started = AtomicBool::new(false);
        let outcome = deadline(request.timeout_ms, &started, async {
            let (dataset, info, schema) = self.mutation_snapshot(ns).await?;
            let ids = validate_ids(request.rows.iter().map(|r| &r.id), info.id_type)?;
            let condition = request.condition.as_deref().unwrap_or("true");
            validate_condition(
                condition,
                &DFSchema::try_from(schema.as_ref().clone()).map_err(invalid)?,
            )?;
            let batch = patch_batch(request, &info, &schema)?;
            let predicate = format!("({}) AND ({condition})", id_filter(&ids));
            let job = {
                let mut builder = lance::dataset::UpdateBuilder::new(Arc::clone(&dataset))
                    .conflict_retries(0)
                    .update_where(&predicate)
                    .map_err(invalid)?;
                // A function root delegates to Lance's full DataFusion expression
                // planner, which supports CASE even though its legacy root parser does not.
                let dialect = datafusion::sql::unparser::dialect::CustomDialectBuilder::new()
                    .with_identifier_quote_style('`')
                    .build();
                let unparser = datafusion::sql::unparser::Unparser::new(&dialect);
                for (column, field) in batch.schema().fields().iter().enumerate().skip(1) {
                    let mut expression = String::from("CASE");
                    for (i, id) in ids.iter().enumerate() {
                        let scalar = datafusion::common::ScalarValue::try_from_array(
                            batch.column(column),
                            i,
                        )
                        .map_err(invalid)?;
                        let literal = unparser
                            .expr_to_sql(&Expr::Literal(scalar, None))
                            .map_err(invalid)?
                            .to_string();
                        expression
                            .push_str(&format!(" WHEN id = {} THEN {literal}", id_literal(id)));
                    }
                    expression.push_str(" END");
                    builder = builder
                        .set(field.name(), &format!("coalesce({expression}, NULL)"))
                        .map_err(invalid)?;
                }
                builder.build().map_err(invalid)?
            };
            matched_ids(&dataset, &id_filter(&ids), ids.len()).await?;
            let affected_ids = matched_ids(&dataset, &predicate, ids.len()).await?;
            if affected_ids.is_empty() {
                return Ok(PatchResult {
                    patched: 0,
                    affected_ids,
                    version: dataset.version_id(),
                });
            }
            started.store(true, Ordering::SeqCst);
            let result = job.execute().await.map_err(execute_error)?;
            if result.rows_updated != affected_ids.len() as u64 {
                return Err(HevSearchError::MutationUnknown(
                    "native patch counts disagree with pinned guard effects".into(),
                ));
            }
            Ok(PatchResult {
                patched: result.rows_updated,
                affected_ids,
                version: result.new_dataset.version_id(),
            })
        })
        .await;
        self.evict_handle(ns);
        self.schema_info.remove(ns);
        outcome
    }

    /// Replace complete postimages only when a native current/incoming predicate
    /// matches. No absent-row insertion occurs unless explicitly requested.
    pub async fn guarded_merge(
        &self,
        ns: &NamespaceId,
        request: &GuardedMergeRequest,
    ) -> Result<GuardedMergeResult, HevSearchError> {
        input_budget(request, request.rows.len(), request.timeout_ms)?;
        let started = AtomicBool::new(false);
        let work = async {
            let (dataset, info, _target_schema) = self.mutation_snapshot(ns).await?;
            let ids = validate_ids(request.rows.iter().map(|r| &r.id), info.id_type)?;
            for row in &request.rows {
                if row.vector.is_empty() && row.vectors.is_none() {
                    if !info.vector_nullable {
                        return Err(HevSearchError::Unsupported(
                            "metadata-only postimage requires nullable vectors".into(),
                        ));
                    }
                } else {
                    validate_row_against(row, info.kind, info.dim)?;
                }
                if row
                    .vector
                    .iter()
                    .chain(row.vectors.iter().flatten().flatten())
                    .any(|v| !v.is_finite())
                {
                    return Err(invalid("vector values must be finite"));
                }
                for field in row.attributes.keys() {
                    if !info.attributes.contains_key(field) {
                        return Err(invalid(format!(
                            "declare attribute {field:?} before guarded merge"
                        )));
                    }
                }
            }
            let attrs = infer_request_attributes(&request.rows)?;
            let mut profile = info.clone();
            reconcile_attribute_schema(&mut profile, &attrs)?;
            let source_schema = Self::schema_for_kind(
                info.kind,
                info.dim,
                info.id_type,
                info.distance_metric,
                info.has_ingested_at,
                info.has_text_tok,
                &info.attributes,
            );
            let batch = rows_to_batch(
                &source_schema,
                info.kind,
                info.dim,
                request.rows.clone(),
                info.has_ingested_at,
                info.has_text_tok,
            )?;
            apply_native_merge(
                &dataset,
                batch,
                &request.condition,
                request.insert_if_missing,
                &ids,
                &started,
            )
            .await
        };
        let outcome = deadline(request.timeout_ms, &started, work).await;
        self.evict_handle(ns);
        self.schema_info.remove(ns);
        outcome
    }
}

async fn matched_ids(
    dataset: &Arc<lance::Dataset>,
    predicate: &str,
    cap: usize,
) -> Result<Vec<RowId>, HevSearchError> {
    let mut scan = dataset.scan();
    scan.project(&["id"]).map_err(invalid)?;
    scan.filter(predicate).map_err(invalid)?;
    scan.limit(Some((cap + 1) as i64), None).map_err(invalid)?;
    let mut stream = scan.try_into_stream().await.map_err(invalid)?;
    let mut ids = Vec::new();
    let mut unique = HashSet::new();
    while let Some(batch) = stream.try_next().await.map_err(invalid)? {
        for id in extract_row_ids(&batch, "mutation affected IDs")? {
            if ids.len() == cap || !unique.insert(id.clone()) {
                return Err(invalid("target IDs are not unique"));
            }
            ids.push(id);
        }
    }
    ids.sort_by_key(RowId::to_sql_literal);
    Ok(ids)
}

async fn apply_native_merge(
    dataset: &Arc<lance::Dataset>,
    batch: RecordBatch,
    condition: &str,
    insert_if_missing: bool,
    ids: &[RowId],
    started: &AtomicBool,
) -> Result<GuardedMergeResult, HevSearchError> {
    let target_schema = Schema::from(dataset.schema());
    let source_schema = batch.schema();
    if insert_if_missing
        && target_schema
            .field_with_name("id")
            .map_err(invalid)?
            .metadata()
            .get("lance-schema:unenforced-primary-key")
            .map(String::as_str)
            != Some("true")
    {
        return Err(HevSearchError::Unsupported("guarded inserts require primary-key conflict metadata; legacy tables need an explicit migration".into()));
    }

    let guard_schema = DFSchema::try_from_qualified_schema("target", &target_schema)
        .map_err(invalid)?
        .join(
            &DFSchema::try_from_qualified_schema("source", source_schema.as_ref())
                .map_err(invalid)?,
        )
        .map_err(invalid)?;
    validate_condition(condition, &guard_schema)?;
    // Native merge uses struct-qualified target/source rows. Validate that
    // exact Lance SQL subset as well as DataFusion typing before dispatch.
    let combined = Schema::new(vec![
        Field::new(
            "target",
            DataType::Struct(target_schema.fields().clone()),
            false,
        ),
        Field::new(
            "source",
            DataType::Struct(source_schema.fields().clone()),
            false,
        ),
    ]);
    let planner = lance::io::exec::Planner::new(Arc::new(combined));
    let native = planner.parse_filter(condition).map_err(invalid)?;
    let native = planner.optimize_expr(native).map_err(invalid)?;
    planner.create_physical_expr(&native).map_err(invalid)?;

    // Bounded point-ID read; duplicate persisted identities are rejected before dispatch.
    matched_ids(dataset, &id_filter(ids), ids.len()).await?;
    let mut builder =
        lance::dataset::MergeInsertBuilder::try_new(Arc::clone(dataset), vec!["id".into()])
            .map_err(invalid)?;
    builder
        .when_matched(lance::dataset::WhenMatched::update_if(dataset, condition).map_err(invalid)?)
        .when_not_matched(if insert_if_missing {
            lance::dataset::WhenNotMatched::InsertAll
        } else {
            lance::dataset::WhenNotMatched::DoNothing
        })
        .conflict_retries(0)
        .skip_auto_cleanup(true);
    let job = builder.try_build().map_err(invalid)?;
    // Lance 6 cannot EXPLAIN partial-schema/scalar-index merge paths. Read
    // plans are exposed by /aggregate; do not pretend a diagnostic limitation
    // means a native patch is unsupported. Guards/schema are prevalidated here.
    let ctx = crate::aggregate::context()?;
    ctx.register_table(
        "target",
        Arc::new(lance::datafusion::LanceTableProvider::new(
            Arc::clone(dataset),
            false,
            false,
        )),
    )
    .map_err(invalid)?;
    ctx.register_table(
        "source",
        Arc::new(
            MemTable::try_new(Arc::clone(&source_schema), vec![vec![batch.clone()]])
                .map_err(invalid)?,
        ),
    )
    .map_err(invalid)?;
    let sql=format!("SELECT source.id AS id FROM source LEFT JOIN (SELECT * FROM target WHERE {}) AS target ON source.id=target.id WHERE (target.id IS NULL AND {insert_if_missing}) OR (target.id IS NOT NULL AND ({condition})) ORDER BY source.id",id_filter(ids));
    let mut stream = ctx
        .sql(&sql)
        .await
        .map_err(invalid)?
        .limit(0, Some(ids.len() + 1))
        .map_err(invalid)?
        .execute_stream()
        .await
        .map_err(invalid)?;
    let mut affected_ids = Vec::new();
    while let Some(batch) = stream.try_next().await.map_err(invalid)? {
        affected_ids.extend(extract_row_ids(&batch, "guarded merge")?);
        if affected_ids.len() > ids.len() {
            return Err(invalid("target IDs are not unique"));
        }
    }
    if affected_ids.is_empty() {
        return Ok(GuardedMergeResult {
            inserted: 0,
            updated: 0,
            skipped: ids.len() as u64,
            affected_ids,
            version: dataset.version_id(),
        });
    }
    let reader: Box<dyn RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(vec![Ok(batch)], source_schema));
    started.store(true, Ordering::SeqCst);
    let (new_dataset, stats) = job.execute_reader(reader).await.map_err(execute_error)?;
    if stats.num_inserted_rows + stats.num_updated_rows != affected_ids.len() as u64
        || stats.num_deleted_rows != 0
    {
        return Err(HevSearchError::MutationUnknown(
            "native merge counts disagree with pinned guard effects".into(),
        ));
    }
    Ok(GuardedMergeResult {
        inserted: stats.num_inserted_rows,
        updated: stats.num_updated_rows,
        skipped: (ids.len() - affected_ids.len()) as u64,
        affected_ids,
        version: new_dataset.version_id(),
    })
}

fn patch_batch(
    request: &PatchRequest,
    info: &NamespaceSchemaInfo,
    schema: &Schema,
) -> Result<RecordBatch, HevSearchError> {
    let fields = request.rows[0].set.keys().cloned().collect::<Vec<_>>();
    if fields.is_empty() || request.rows.iter().any(|r| r.set.keys().ne(fields.iter())) {
        return Err(invalid("atomic patch rows must assign the same nonempty field set; heterogeneous field sets are unsupported"));
    }
    let rows = request
        .rows
        .iter()
        .map(|row| UpsertRow {
            id: row.id.clone(),
            vector: Vec::new(),
            vectors: None,
            text: row
                .set
                .get("text")
                .and_then(Value::as_str)
                .map(str::to_string),
            attributes: serde_json::Map::from_iter(
                row.set
                    .iter()
                    .filter(|(name, _)| !["text", "vector", "vectors"].contains(&name.as_str()))
                    .map(|(k, v)| (k.clone(), v.clone())),
            ),
        })
        .collect::<Vec<_>>();
    let ids: ArrayRef = match info.id_type {
        RowIdType::String => Arc::new(StringArray::from(
            rows.iter()
                .map(|r| match &r.id {
                    RowId::String(v) => v.as_str(),
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>(),
        )),
        RowIdType::U64 => Arc::new(UInt64Array::from_iter_values(rows.iter().map(
            |r| match r.id {
                RowId::U64(v) => v,
                _ => unreachable!(),
            },
        ))),
    };
    let mut columns = vec![ids];
    let mut output_fields = vec![Arc::new(
        schema.field_with_name("id").map_err(invalid)?.clone(),
    )];
    // Build only incoming columns. Native partial-schema merge preserves every
    // omitted field without collecting existing rows or manufacturing postimages.
    for name in fields {
        let physical = if name == "vectors" {
            "vector"
        } else {
            name.as_str()
        };
        let array: ArrayRef = if name == "text" {
            if request
                .rows
                .iter()
                .any(|r| !r.set[&name].is_null() && !r.set[&name].is_string())
            {
                return Err(invalid("text must be string or null"));
            }
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.text.as_deref()).collect::<Vec<_>>(),
            ))
        } else if name == "vector" || name == "vectors" {
            if (info.kind == VectorKind::Single && name != "vector")
                || (info.kind == VectorKind::Multivector && name != "vectors")
            {
                return Err(invalid("vector field does not match namespace kind"));
            }
            if info.dim > 65536 {
                return Err(HevSearchError::Unsupported(
                    "vector patch dimensions above 65536 are unsupported".into(),
                ));
            }
            let mut vector_rows = Vec::new();
            for input in &request.rows {
                let value = &input.set[&name];
                let mut row = UpsertRow {
                    id: input.id.clone(),
                    vector: Vec::new(),
                    vectors: None,
                    text: None,
                    attributes: serde_json::Map::new(),
                };
                if value.is_null() {
                    return Err(HevSearchError::Unsupported(
                        "clearing vectors in native updates is unsupported by pinned Lance; no effects were dispatched".into(),
                    ));
                } else {
                    let parse = |v: &Value| -> Result<Vec<f32>, HevSearchError> {
                        v.as_array()
                            .ok_or_else(|| invalid("vectors must be arrays"))?
                            .iter()
                            .map(|n| {
                                let n = n
                                    .as_f64()
                                    .ok_or_else(|| invalid("vector elements must be numbers"))?
                                    as f32;
                                if !n.is_finite() {
                                    return Err(invalid("vector elements must be finite float32"));
                                }
                                Ok(n)
                            })
                            .collect()
                    };
                    if info.kind == VectorKind::Single {
                        row.vector = parse(value)?;
                    } else {
                        row.vectors = Some(
                            value
                                .as_array()
                                .ok_or_else(|| invalid("vectors must be arrays"))?
                                .iter()
                                .map(parse)
                                .collect::<Result<Vec<_>, _>>()?,
                        );
                    }
                    validate_row_against(&row, info.kind, info.dim)?;
                }
                vector_rows.push(row);
            }
            let vector_schema = NamespaceManager::schema_for_kind(
                info.kind,
                info.dim,
                info.id_type,
                info.distance_metric,
                false,
                false,
                &BTreeMap::new(),
            );
            let batch = rows_to_batch(
                &vector_schema,
                info.kind,
                info.dim,
                vector_rows,
                false,
                false,
            )?;
            Arc::clone(batch.column_by_name("vector").expect("canonical vector"))
        } else {
            validate_attribute_name(&name)?;
            let ty = info
                .attributes
                .get(&name)
                .ok_or_else(|| invalid(format!("declare attribute {name:?} before patch")))?;
            build_attribute_array(&rows, &name, *ty)?
        };
        let field = schema.field_with_name(physical).map_err(invalid)?.clone();
        let array = if array.data_type() != field.data_type() {
            arrow_cast::cast(&array, field.data_type()).map_err(invalid)?
        } else {
            array
        };
        output_fields.push(Arc::new(field));
        columns.push(array);
    }
    if request.rows[0].set.contains_key("text") && info.has_text_tok {
        output_fields.push(Arc::new(
            schema
                .field_with_name(TEXT_TOK_COLUMN)
                .map_err(invalid)?
                .clone(),
        ));
        columns.push(Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.text.as_deref().and_then(crate::analyzer::tokenized))
                .collect::<Vec<_>>(),
        )));
    }
    if info.has_ingested_at {
        output_fields.push(Arc::new(
            schema
                .field_with_name(INGESTED_AT_COLUMN)
                .map_err(invalid)?
                .clone(),
        ));
        columns.push(Arc::new(TimestampMicrosecondArray::from_iter_values(
            std::iter::repeat_n(current_micros(), rows.len()),
        )));
    }
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            output_fields,
            schema.metadata().clone(),
        )),
        columns,
    )
    .map_err(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn deadline_distinguishes_no_dispatch_from_unknown_acceptance() {
        let started = AtomicBool::new(false);
        let result: Result<(), _> = deadline(1, &started, std::future::pending()).await;
        assert!(matches!(result, Err(HevSearchError::InvalidRequest(_))));
        let work = async {
            started.store(true, Ordering::SeqCst);
            std::future::pending::<Result<(), HevSearchError>>().await
        };
        let result = deadline(1, &started, work).await;
        assert!(matches!(result, Err(HevSearchError::MutationUnknown(_))));
    }
}
