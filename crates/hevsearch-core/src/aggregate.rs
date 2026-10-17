//! Bounded, read-only SQL over native Lance table providers.
//!
//! The executor consumes Arrow streams from the physical plan. Only bounded
//! result batches cross into JSON; source rows never enter a Rust aggregation.

use std::{collections::BTreeMap, sync::Arc, time::Instant};

use datafusion::{
    execution::runtime_env::RuntimeEnvBuilder,
    physical_plan::{display::DisplayableExecutionPlan, execute_stream},
    prelude::{SQLOptions, SessionConfig, SessionContext},
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

use crate::{HevSearchError, NamespaceId};

/// Hard result row ceiling, independent of a SQL LIMIT.
pub const MAX_ROWS: usize = 10_000;
/// Hard serialized result byte ceiling.
pub const MAX_BYTES: usize = 8 * 1024 * 1024;

/// Request-local aliases map to physical namespaces, never paths or URLs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateRequest {
    /// One to eight namespace bindings. Aliases are simple lower-case identifiers.
    pub tables: BTreeMap<String, String>,
    /// One read-only SELECT/CTE query, in the pinned DataFusion dialect.
    pub sql: String,
    /// Optional physical namespace-to-version pins for consistent repeated reads.
    /// Missing/expired versions fail; they never fall back to the latest version.
    #[serde(default)]
    pub versions: BTreeMap<String, u64>,
    /// Result row budget; exceeding it fails instead of returning partial totals.
    pub max_rows: usize,
    /// Serialized JSON row budget, at most 8 MiB.
    pub max_bytes: usize,
    /// End-to-end planning/execution deadline, in milliseconds (1..=60000).
    pub timeout_ms: u64,
    /// Return plans without scanning data. Defaults to false.
    #[serde(default)]
    pub explain_only: bool,
}

impl AggregateRequest {
    /// Validate budgets and aliases before resolving any table.
    pub fn validate(&self) -> Result<(), HevSearchError> {
        if self.tables.is_empty() || self.tables.len() > 8 {
            return Err(invalid("tables must contain 1..=8 bindings"));
        }
        for (alias, namespace) in &self.tables {
            if alias.is_empty()
                || alias.len() > 64
                || !alias.as_bytes()[0].is_ascii_lowercase()
                || !alias
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            {
                return Err(invalid("table aliases must be lower-case SQL identifiers"));
            }
            NamespaceId::new(namespace.clone())?;
        }
        for (namespace, version) in &self.versions {
            if *version == 0 || !self.tables.values().any(|ns| ns == namespace) {
                return Err(invalid(
                    "versions must pin bound namespaces to nonzero versions",
                ));
            }
        }
        if self.sql.is_empty() || self.sql.len() > 64 * 1024 {
            return Err(invalid("sql must contain 1..=65536 bytes"));
        }
        if !(1..=MAX_ROWS).contains(&self.max_rows)
            || !(1..=MAX_BYTES).contains(&self.max_bytes)
            || !(1..=60_000).contains(&self.timeout_ms)
        {
            return Err(invalid("aggregate budget out of range"));
        }
        Ok(())
    }
}

/// Exact physical snapshot used by one alias. Multiple tables are not an atomic snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateSnapshot {
    /// Physical namespace.
    pub namespace: String,
    /// Lance version captured before planning.
    pub version: u64,
}

/// Bounded result with actual optimized and physical plans and execution metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateResult {
    /// JSON objects; decimal Arrow values use Arrow's exact string representation.
    pub rows: Vec<serde_json::Value>,
    /// Alias-to-snapshot binding for reproducible reads.
    pub snapshots: BTreeMap<String, AggregateSnapshot>,
    /// Optimized logical plan.
    pub logical_plan: String,
    /// Actual physical plan, with per-operator metrics after execution.
    pub physical_plan: String,
    /// Wall time, including planning and serialization (milliseconds).
    pub elapsed_ms: u64,
    /// Serialized rows size; excludes plan metadata.
    pub result_bytes: usize,
    /// False for EXPLAIN-only requests.
    pub executed: bool,
}

pub(crate) fn invalid(message: impl ToString) -> HevSearchError {
    HevSearchError::InvalidRequest(message.to_string())
}

/// A fresh executor has no external tables, URL readers, or mutable SQL state.
pub(crate) fn context() -> Result<SessionContext, HevSearchError> {
    context_with_budgets(256 * 1024 * 1024, 512 * 1024 * 1024)
}

fn context_with_budgets(memory: usize, spill: u64) -> Result<SessionContext, HevSearchError> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(memory, 1.0)
        .with_max_temp_directory_size(spill)
        .build()
        .map_err(|e| HevSearchError::Backend(e.to_string()))?;
    Ok(SessionContext::new_with_config_rt(
        SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(1024),
        Arc::new(runtime),
    ))
}

pub(crate) async fn execute(
    ctx: SessionContext,
    request: &AggregateRequest,
    snapshots: BTreeMap<String, AggregateSnapshot>,
    started: Instant,
) -> Result<AggregateResult, HevSearchError> {
    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);
    let df = ctx
        .sql_with_options(&request.sql, options)
        .await
        .map_err(invalid)?;
    // Reject EXPLAIN statements too: this service owns EXPLAIN/ANALYZE so the
    // execution flag, budgets and metrics cannot disagree with the actual query.
    if !matches!(
        df.logical_plan(),
        datafusion::logical_expr::LogicalPlan::Projection(_)
            | datafusion::logical_expr::LogicalPlan::Limit(_)
            | datafusion::logical_expr::LogicalPlan::Sort(_)
            | datafusion::logical_expr::LogicalPlan::Aggregate(_)
            | datafusion::logical_expr::LogicalPlan::Union(_)
            | datafusion::logical_expr::LogicalPlan::SubqueryAlias(_)
            | datafusion::logical_expr::LogicalPlan::Distinct(_)
    ) {
        return Err(invalid("aggregate requires a SELECT query"));
    }
    let mut names = std::collections::HashSet::new();
    for field in df.schema().fields() {
        if !names.insert(field.name()) {
            return Err(invalid(
                "output column names must be unique; use AS aliases",
            ));
        }
    }
    let df = df.limit(0, Some(request.max_rows + 1)).map_err(invalid)?;
    let logical_plan = df
        .clone()
        .into_optimized_plan()
        .map_err(invalid)?
        .display_indent()
        .to_string();
    let plan = df.create_physical_plan().await.map_err(invalid)?;
    let mut rows = Vec::new();
    let mut result_bytes = 2;
    if !request.explain_only {
        let mut stream = execute_stream(Arc::clone(&plan), ctx.task_ctx()).map_err(invalid)?;
        while let Some(batch) = stream.try_next().await.map_err(invalid)? {
            if rows.len() + batch.num_rows() > request.max_rows {
                return Err(invalid(
                    "aggregate result exceeds max_rows; add grouping or a tighter bound",
                ));
            }
            // Bound Arrow output before encoding potentially wide/list values.
            if batch.get_array_memory_size() > MAX_BYTES * 4 {
                return Err(invalid("aggregate output batch exceeds memory budget"));
            }
            // JSON has no decimal type: preserve scalar financial values as
            // exact strings rather than round them through serde_json f64.
            let mut columns = batch.columns().to_vec();
            let mut fields = batch.schema().fields().to_vec();
            for i in 0..columns.len() {
                if matches!(
                    columns[i].data_type(),
                    arrow_schema::DataType::Decimal32(_, _)
                        | arrow_schema::DataType::Decimal64(_, _)
                        | arrow_schema::DataType::Decimal128(_, _)
                        | arrow_schema::DataType::Decimal256(_, _)
                ) {
                    columns[i] = arrow_cast::cast(&columns[i], &arrow_schema::DataType::Utf8)
                        .map_err(invalid)?;
                    fields[i] = Arc::new(arrow_schema::Field::new(
                        fields[i].name(),
                        arrow_schema::DataType::Utf8,
                        true,
                    ));
                }
            }
            let batch = arrow_array::RecordBatch::try_new(
                Arc::new(arrow_schema::Schema::new(fields)),
                columns,
            )
            .map_err(invalid)?;
            let mut writer = arrow_json::WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, arrow_json::writer::JsonArray>(Vec::new());
            writer.write(&batch).map_err(invalid)?;
            writer.finish().map_err(invalid)?;
            let encoded = writer.into_inner();
            let values: Vec<serde_json::Value> =
                serde_json::from_slice(&encoded).map_err(invalid)?;
            for value in values {
                result_bytes += serde_json::to_vec(&value).map_err(invalid)?.len()
                    + usize::from(!rows.is_empty());
                if result_bytes > request.max_bytes {
                    return Err(invalid("aggregate result exceeds max_bytes"));
                }
                rows.push(value);
            }
        }
    }
    if result_bytes > request.max_bytes {
        return Err(invalid("aggregate result exceeds max_bytes"));
    }
    let physical_plan = DisplayableExecutionPlan::with_full_metrics(plan.as_ref())
        .indent(true)
        .to_string();
    if logical_plan.len() + physical_plan.len() > 1024 * 1024 {
        return Err(invalid("aggregate plan output exceeds 1 MiB"));
    }
    Ok(AggregateResult {
        rows,
        snapshots,
        logical_plan,
        physical_plan,
        elapsed_ms: started.elapsed().as_millis() as u64,
        result_bytes,
        executed: !request.explain_only,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_and_spill_limits_reject_native_sort_without_partial_output() {
        // Same constructor as production, with small budgets to exercise actual
        // DataFusion reservations/spill failure without a large shared-host load.
        for (memory, spill) in [(8192, 0), (8 * 1024 * 1024, 1)] {
            let request=AggregateRequest {
                tables:BTreeMap::from([("unused".into(),"synthetic".into())]),
                versions:BTreeMap::new(),
                sql:"SELECT row_number() OVER (ORDER BY concat(repeat('x',256), CAST(value AS VARCHAR))) AS n FROM generate_series(1,100000) AS x(value)".into(),
                max_rows:1,max_bytes:4096,timeout_ms:5000,explain_only:false,
            };
            let error = execute(
                context_with_budgets(memory, spill).unwrap(),
                &request,
                BTreeMap::new(),
                Instant::now(),
            )
            .await
            .unwrap_err()
            .to_string();
            let lower = error.to_lowercase();
            if spill == 0 {
                assert!(
                    lower.contains("resource") || lower.contains("memory"),
                    "{error}"
                );
            } else {
                assert!(
                    lower.contains("spill")
                        || lower.contains("disk")
                        || lower.contains("temporary"),
                    "{error}"
                );
            }
        }
    }
}
