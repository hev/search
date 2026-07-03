package client

import "encoding/json"

// These types mirror the engine's REST JSON shapes
// (crates/hevsearch-core/src/result.rs and query.rs). Field names match
// the wire exactly so json mode can pass responses through unchanged.

// NamespaceList is the body of GET /ns.
type NamespaceList struct {
	Namespaces []string `json:"namespaces"`
}

// NamespaceInfo is the body of GET /ns/{ns}.
type NamespaceInfo struct {
	Namespace      string `json:"namespace"`
	Kind           string `json:"kind"`
	VectorDim      int    `json:"vector_dim"`
	IDType         string `json:"id_type"`
	DistanceMetric string `json:"distance_metric"`
	RowCount       int    `json:"row_count"`
	FragmentCount  int    `json:"fragment_count"`
	HasVectorIndex bool   `json:"has_vector_index"`
	HasFtsIndex    bool   `json:"has_fts_index"`
	HasScalarIndex bool   `json:"has_scalar_index"`
	TableVersion   uint64 `json:"table_version"`
}

// ListRow is one row from GET /ns/{ns}/list.
type ListRow struct {
	ID               json.RawMessage        `json:"id"`
	Vector           []float32              `json:"vector,omitempty"`
	Text             *string                `json:"text,omitempty"`
	IngestedAtMicros int64                  `json:"ingested_at_micros"`
	Attributes       map[string]interface{} `json:"attributes,omitempty"`
}

// ListPage is the body of GET /ns/{ns}/list.
type ListPage struct {
	Rows       []ListRow `json:"rows"`
	NextCursor *string   `json:"next_cursor"`
}

// FuzzyRequest is the fuzzy sub-object of a query.
type FuzzyRequest struct {
	MaxEditDistance interface{} `json:"max_edit_distance"`
}

// QueryRequest is the body of POST /ns/{ns}/query.
type QueryRequest struct {
	Vector        []float32     `json:"vector,omitempty"`
	Vectors       [][]float32   `json:"vectors,omitempty"`
	K             int           `json:"k"`
	Nprobes       *int          `json:"nprobes,omitempty"`
	Text          *string       `json:"text,omitempty"`
	Fuzzy         *FuzzyRequest `json:"fuzzy,omitempty"`
	Filter        *string       `json:"filter,omitempty"`
	IncludeVector bool          `json:"include_vector"`
}

// QueryResult is one hit from POST /ns/{ns}/query.
type QueryResult struct {
	ID               json.RawMessage        `json:"id"`
	Score            float32                `json:"score"`
	Vector           []float32              `json:"vector,omitempty"`
	Text             *string                `json:"text,omitempty"`
	IngestedAtMicros *int64                 `json:"ingested_at_micros,omitempty"`
	Attributes       map[string]interface{} `json:"attributes,omitempty"`
}

// QueryResultSet is the body of POST /ns/{ns}/query.
type QueryResultSet struct {
	QueryID string        `json:"query_id"`
	Results []QueryResult `json:"results"`
}

// FacetRequest is the body of POST /ns/{ns}/facet.
type FacetRequest struct {
	Filter *string  `json:"filter,omitempty"`
	Fields []string `json:"fields"`
	Top    *int     `json:"top,omitempty"`
}

// FacetBucket is one value-count bucket.
type FacetBucket struct {
	Value interface{} `json:"value"`
	Count uint64      `json:"count"`
}

// FacetField is the buckets for one field.
type FacetField struct {
	Field     string        `json:"field"`
	Buckets   []FacetBucket `json:"buckets"`
	Truncated bool          `json:"truncated"`
}

// FacetResultSet is the body of POST /ns/{ns}/facet.
type FacetResultSet struct {
	Facets []FacetField `json:"facets"`
}

// UpsertRow is one row of an upsert request.
type UpsertRow struct {
	ID         interface{}            `json:"id"`
	Vector     []float32              `json:"vector,omitempty"`
	Vectors    [][]float32            `json:"vectors,omitempty"`
	Text       *string                `json:"text,omitempty"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

// UpsertRequest is the body of POST /ns/{ns}/upsert.
type UpsertRequest struct {
	DistanceMetric *string     `json:"distance_metric,omitempty"`
	Rows           []UpsertRow `json:"rows"`
}

// UpsertResponse is the body of a successful upsert.
type UpsertResponse struct {
	Upserted int `json:"upserted"`
}

// DeleteResponse is the body of DELETE /ns/{ns}.
type DeleteResponse struct {
	ObjectsDeleted int `json:"objects_deleted"`
}

// DeleteRowsRequest is the body of POST /ns/{ns}/delete.
type DeleteRowsRequest struct {
	IDs    []interface{} `json:"ids,omitempty"`
	Filter *string       `json:"filter,omitempty"`
}

// DeleteRowsResponse is the body of a successful row delete.
type DeleteRowsResponse struct {
	Deleted uint64 `json:"deleted"`
}

// IndexRequest is the body of POST /ns/{ns}/index.
type IndexRequest struct {
	Kind          string  `json:"kind"`
	NumPartitions *uint32 `json:"num_partitions,omitempty"`
	NumSubVectors *uint32 `json:"num_sub_vectors,omitempty"`
	NumBits       *uint32 `json:"num_bits,omitempty"`
}

// ScalarIndexRequest is the body of POST /ns/{ns}/scalar-index.
type ScalarIndexRequest struct {
	Column string `json:"column"`
}

// OperationAccepted is the 202 body from index/fts-index/scalar-index/compact.
type OperationAccepted struct {
	OperationID string `json:"operation_id"`
	Kind        string `json:"kind"`
	Namespace   string `json:"namespace"`
	Status      string `json:"status"`
}

// WarmupRequest is the body of POST /ns/{ns}/warmup. Queries are held as
// raw JSON so the exact query shape the operator wrote (including whether
// include_vector was specified) reaches the engine unchanged — reserializing
// through QueryRequest would inject default fields and split cache keys.
type WarmupRequest struct {
	Queries []json.RawMessage `json:"queries"`
}

// WarmupAccepted is the 202 body from warmup.
type WarmupAccepted struct {
	OperationAccepted
	Queued int `json:"queued"`
}

// OperationRecord is the body of GET /operations/{id}. Fields beyond the
// documented core are captured in Extra so json mode passes them through.
type OperationRecord struct {
	OperationID string          `json:"operation_id"`
	Kind        string          `json:"kind"`
	Namespace   string          `json:"namespace"`
	Status      string          `json:"status"`
	Error       *string         `json:"error,omitempty"`
	Raw         json.RawMessage `json:"-"`
}
