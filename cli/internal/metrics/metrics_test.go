package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sample = `# HELP hevsearch_query_total Total queries
# TYPE hevsearch_query_total counter
hevsearch_query_total 42
hevsearch_cache_hits_total{cache="exact"} 10
hevsearch_cache_misses_total{cache="exact"} 3
hevsearch_s3_requests_total 100
hevsearch_unrelated_gauge 7
`

func TestParseSkipsComments(t *testing.T) {
	got := Parse(sample)
	require.Len(t, got, 5)
	assert.Equal(t, "hevsearch_query_total", got[0].Name)
	assert.Equal(t, float64(42), got[0].Value)
	assert.Equal(t, `{cache="exact"}`, got[1].Labels)
	assert.Equal(t, "hevsearch_cache_hits_total", got[1].Name)
}

func TestCuratedFilter(t *testing.T) {
	got := Filter(Parse(sample), CuratedSubstrings)
	names := map[string]bool{}
	for _, s := range got {
		names[s.Name] = true
	}
	assert.True(t, names["hevsearch_query_total"])
	assert.True(t, names["hevsearch_cache_hits_total"])
	assert.True(t, names["hevsearch_s3_requests_total"])
	assert.False(t, names["hevsearch_unrelated_gauge"], "curated subset should drop unrelated metrics")
}

func TestFilterPrefix(t *testing.T) {
	got := FilterPrefix(Parse(sample), "hevsearch_cache")
	require.Len(t, got, 2)
	for _, s := range got {
		assert.Contains(t, s.Name, "hevsearch_cache")
	}
}
