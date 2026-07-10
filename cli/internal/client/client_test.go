package client

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hev/search/cli/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestClient(url string, ep config.Endpoint) *Client {
	ep.URL = url
	return New(ep)
}

func TestQueryRequestConstruction(t *testing.T) {
	var gotMethod, gotPath, gotContentType string
	var gotBody map[string]interface{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotContentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"query_id":"q1","results":[{"id":7,"score":0.5,"attributes":{}}]}`))
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, config.Endpoint{})
	text := "hello"
	res, raw, err := c.Query(t.Context(), "docs", QueryRequest{K: 5, Text: &text, IncludeVector: false})
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "/ns/docs/query", gotPath)
	assert.Equal(t, "application/json", gotContentType)
	assert.Equal(t, float64(5), gotBody["k"])
	assert.Equal(t, "hello", gotBody["text"])
	assert.Equal(t, false, gotBody["include_vector"])

	assert.Equal(t, "q1", res.QueryID)
	require.Len(t, res.Results, 1)
	assert.Equal(t, "7", string(res.Results[0].ID))
	assert.Equal(t, float32(0.5), res.Results[0].Score)
	assert.Contains(t, string(raw), "q1")
}

func TestAdminEndpointUsesDeleteRoute(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"objects_deleted":3}`))
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, config.Endpoint{})
	res, _, err := c.DeleteNamespace(t.Context(), "docs")
	require.NoError(t, err)
	assert.Equal(t, http.MethodDelete, gotMethod)
	assert.Equal(t, "/ns/docs", gotPath)
	assert.Equal(t, 3, res.ObjectsDeleted)
}

func TestNoAuthHeaderSent(t *testing.T) {
	var hadAuth bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, hadAuth = r.Header["Authorization"]
		_, _ = w.Write([]byte(`{"namespaces":["a","b"]}`))
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, config.Endpoint{})
	list, _, err := c.ListNamespaces(t.Context())
	require.NoError(t, err)
	assert.False(t, hadAuth, "the CLI should not send Authorization headers")
	assert.Equal(t, []string{"a", "b"}, list.Namespaces)
}

func TestListParamsEncoded(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = w.Write([]byte(`{"rows":[],"next_cursor":null}`))
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, config.Endpoint{})
	_, _, err := c.List(t.Context(), "docs", ListParams{Limit: 10, Order: "asc", Filter: "id > 5", Cursor: "abc"})
	require.NoError(t, err)
	assert.Contains(t, gotQuery, "limit=10")
	assert.Contains(t, gotQuery, "order=asc")
	assert.Contains(t, gotQuery, "cursor=abc")
	assert.Contains(t, gotQuery, "filter=id+%3E+5")
}

func TestAPIErrorParsed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid request: bad filter"}`))
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, config.Endpoint{})
	_, _, err := c.Info(t.Context(), "docs")
	require.Error(t, err)
	apiErr, ok := err.(*APIError)
	require.True(t, ok, "expected *APIError, got %T", err)
	assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	assert.Equal(t, "invalid request: bad filter", apiErr.Message)
	assert.Contains(t, apiErr.Error(), "HTTP 400")
}

func TestConnectionRefusedMessage(t *testing.T) {
	// Point at a closed port to force a transport error.
	c := newTestClient("http://127.0.0.1:1", config.Endpoint{})
	_, err := c.Health(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "engine unreachable at http://127.0.0.1:1")
}

func TestScalarIndexNoBodyWhenNil(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"operation_id":"op1","kind":"scalar_index","namespace":"docs","status":"running"}`))
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, config.Endpoint{})
	op, _, err := c.CreateScalarIndex(t.Context(), "docs", nil)
	require.NoError(t, err)
	assert.Empty(t, gotBody, "nil scalar-index request must send no body")
	assert.Equal(t, "op1", op.OperationID)
}
