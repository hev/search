// Package client is a hand-written REST client for the hev search engine's
// internal API. There is no generated SDK; this speaks the axum surface
// documented in crates/hevsearch-api/src/handlers.rs directly.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hev/search/cli/internal/config"
)

// Client talks to one engine endpoint.
type Client struct {
	ep   config.Endpoint
	http *http.Client
}

// New builds a client for the resolved endpoint.
func New(ep config.Endpoint) *Client {
	return &Client{
		ep:   ep,
		http: &http.Client{Timeout: 60 * time.Second},
	}
}

// Endpoint returns the resolved endpoint the client was built with.
func (c *Client) Endpoint() config.Endpoint { return c.ep }

// APIError is a structured engine error carrying the HTTP status and the
// message from the {"error": ...} body.
type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("engine returned HTTP %d", e.Status)
	}
	return fmt.Sprintf("HTTP %d: %s", e.Status, e.Message)
}

type errorBody struct {
	Error string `json:"error"`
}

// doRaw issues a request and returns the raw response body on 2xx, or a
// typed error otherwise.
func (c *Client) doRaw(ctx context.Context, method, path string, body []byte, useAdmin bool) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.ep.URL+path, bytesReader(body))
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, c.transportError(err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return respBody, nil
	}

	apiErr := &APIError{Status: resp.StatusCode}
	var eb errorBody
	if json.Unmarshal(respBody, &eb) == nil && eb.Error != "" {
		apiErr.Message = eb.Error
	} else if len(respBody) > 0 {
		apiErr.Message = strings.TrimSpace(string(respBody))
	}
	return nil, apiErr
}

// transportError turns a connection failure into a clear operator message.
func (c *Client) transportError(err error) error {
	if errors.Is(err, syscall.ECONNREFUSED) || strings.Contains(err.Error(), "connection refused") {
		return fmt.Errorf("engine unreachable at %s (connection refused)", c.ep.URL)
	}
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "timeout") {
		return fmt.Errorf("engine unreachable at %s (timed out)", c.ep.URL)
	}
	return fmt.Errorf("request to %s failed: %w", c.ep.URL, err)
}

func bytesReader(b []byte) io.Reader {
	if b == nil {
		return nil
	}
	return bytes.NewReader(b)
}

// doJSON issues a request, returns the raw 2xx body, and unmarshals it
// into out when out is non-nil.
func (c *Client) doJSON(ctx context.Context, method, path string, in any, out any, useAdmin bool) ([]byte, error) {
	var body []byte
	if in != nil {
		var err error
		body, err = json.Marshal(in)
		if err != nil {
			return nil, err
		}
	}
	raw, err := c.doRaw(ctx, method, path, body, useAdmin)
	if err != nil {
		return nil, err
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return raw, fmt.Errorf("decoding response: %w", err)
		}
	}
	return raw, nil
}

// --- Read-path endpoints ---

// Health returns the /health probe body ("ok").
func (c *Client) Health(ctx context.Context) (string, error) {
	raw, err := c.doRaw(ctx, http.MethodGet, "/health", nil, false)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(raw)), nil
}

// Metrics returns the raw Prometheus text from /metrics.
func (c *Client) Metrics(ctx context.Context) (string, error) {
	raw, err := c.doRaw(ctx, http.MethodGet, "/metrics", nil, false)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// ListNamespaces returns the namespaces from GET /ns.
func (c *Client) ListNamespaces(ctx context.Context) (NamespaceList, []byte, error) {
	var out NamespaceList
	raw, err := c.doJSON(ctx, http.MethodGet, "/ns", nil, &out, false)
	return out, raw, err
}

// Info returns namespace metadata from GET /ns/{ns}.
func (c *Client) Info(ctx context.Context, ns string) (NamespaceInfo, []byte, error) {
	var out NamespaceInfo
	raw, err := c.doJSON(ctx, http.MethodGet, "/ns/"+url.PathEscape(ns), nil, &out, false)
	return out, raw, err
}

// ListParams are the query parameters for GET /ns/{ns}/list.
type ListParams struct {
	OrderBy string
	Order   string
	Limit   int
	Cursor  string
	Filter  string
}

// List returns a page of rows from GET /ns/{ns}/list.
func (c *Client) List(ctx context.Context, ns string, p ListParams) (ListPage, []byte, error) {
	q := url.Values{}
	if p.OrderBy != "" {
		q.Set("order_by", p.OrderBy)
	}
	if p.Order != "" {
		q.Set("order", p.Order)
	}
	if p.Limit > 0 {
		q.Set("limit", strconv.Itoa(p.Limit))
	}
	if p.Cursor != "" {
		q.Set("cursor", p.Cursor)
	}
	if p.Filter != "" {
		q.Set("filter", p.Filter)
	}
	path := "/ns/" + url.PathEscape(ns) + "/list"
	if enc := q.Encode(); enc != "" {
		path += "?" + enc
	}
	var out ListPage
	raw, err := c.doJSON(ctx, http.MethodGet, path, nil, &out, false)
	return out, raw, err
}

// Query runs a search via POST /ns/{ns}/query.
func (c *Client) Query(ctx context.Context, ns string, req QueryRequest) (QueryResultSet, []byte, error) {
	var out QueryResultSet
	raw, err := c.doJSON(ctx, http.MethodPost, "/ns/"+url.PathEscape(ns)+"/query", req, &out, false)
	return out, raw, err
}

// Facet computes facets via POST /ns/{ns}/facet.
func (c *Client) Facet(ctx context.Context, ns string, req FacetRequest) (FacetResultSet, []byte, error) {
	var out FacetResultSet
	raw, err := c.doJSON(ctx, http.MethodPost, "/ns/"+url.PathEscape(ns)+"/facet", req, &out, false)
	return out, raw, err
}

// Upsert appends rows via POST /ns/{ns}/upsert.
func (c *Client) Upsert(ctx context.Context, ns string, req UpsertRequest) (UpsertResponse, []byte, error) {
	var out UpsertResponse
	raw, err := c.doJSON(ctx, http.MethodPost, "/ns/"+url.PathEscape(ns)+"/upsert", req, &out, false)
	return out, raw, err
}

// GetOperation polls GET /operations/{id}.
func (c *Client) GetOperation(ctx context.Context, id string) (OperationRecord, []byte, error) {
	var out OperationRecord
	raw, err := c.doJSON(ctx, http.MethodGet, "/operations/"+url.PathEscape(id), nil, &out, false)
	if raw != nil {
		out.Raw = raw
	}
	return out, raw, err
}

// Warmup enqueues cache-warmup queries via POST /ns/{ns}/warmup.
func (c *Client) Warmup(ctx context.Context, ns string, req WarmupRequest) (WarmupAccepted, []byte, error) {
	var out WarmupAccepted
	raw, err := c.doJSON(ctx, http.MethodPost, "/ns/"+url.PathEscape(ns)+"/warmup", req, &out, false)
	return out, raw, err
}

// --- Admin-path endpoints ---

// DeleteNamespace removes a namespace via DELETE /ns/{ns}.
func (c *Client) DeleteNamespace(ctx context.Context, ns string) (DeleteResponse, []byte, error) {
	var out DeleteResponse
	raw, err := c.doJSON(ctx, http.MethodDelete, "/ns/"+url.PathEscape(ns), nil, &out, true)
	return out, raw, err
}

// DeleteRows removes rows via POST /ns/{ns}/delete.
func (c *Client) DeleteRows(ctx context.Context, ns string, req DeleteRowsRequest) (DeleteRowsResponse, []byte, error) {
	var out DeleteRowsResponse
	raw, err := c.doJSON(ctx, http.MethodPost, "/ns/"+url.PathEscape(ns)+"/delete", req, &out, true)
	return out, raw, err
}

// CreateIndex builds an ANN index via POST /ns/{ns}/index.
func (c *Client) CreateIndex(ctx context.Context, ns string, req IndexRequest) (OperationAccepted, []byte, error) {
	var out OperationAccepted
	raw, err := c.doJSON(ctx, http.MethodPost, "/ns/"+url.PathEscape(ns)+"/index", req, &out, true)
	return out, raw, err
}

// CreateFtsIndex builds a BM25 index via POST /ns/{ns}/fts-index.
func (c *Client) CreateFtsIndex(ctx context.Context, ns string) (OperationAccepted, []byte, error) {
	var out OperationAccepted
	raw, err := c.doJSON(ctx, http.MethodPost, "/ns/"+url.PathEscape(ns)+"/fts-index", nil, &out, true)
	return out, raw, err
}

// CreateScalarIndex builds a BTree index via POST /ns/{ns}/scalar-index.
// A nil req sends no body (engine defaults to _ingested_at).
func (c *Client) CreateScalarIndex(ctx context.Context, ns string, req *ScalarIndexRequest) (OperationAccepted, []byte, error) {
	var out OperationAccepted
	var in any
	if req != nil {
		in = req
	}
	raw, err := c.doJSON(ctx, http.MethodPost, "/ns/"+url.PathEscape(ns)+"/scalar-index", in, &out, true)
	return out, raw, err
}

// Compact merges data files via POST /ns/{ns}/compact.
func (c *Client) Compact(ctx context.Context, ns string) (OperationAccepted, []byte, error) {
	var out OperationAccepted
	raw, err := c.doJSON(ctx, http.MethodPost, "/ns/"+url.PathEscape(ns)+"/compact", nil, &out, true)
	return out, raw, err
}
