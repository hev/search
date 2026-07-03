package output

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// capture redirects stdout for the duration of fn and returns what it wrote.
func capture(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	fn()
	_ = w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	return buf.String()
}

func TestResolveExplicit(t *testing.T) {
	m, err := Resolve("json")
	require.NoError(t, err)
	assert.Equal(t, ModeJSON, m)

	m, err = Resolve("plain")
	require.NoError(t, err)
	assert.Equal(t, ModePlain, m)

	_, err = Resolve("yaml")
	assert.Error(t, err)
}

func TestPrintTablePlain(t *testing.T) {
	CurrentMode = ModePlain
	defer func() { CurrentMode = ModeHuman }()

	out := capture(t, func() {
		PrintTable([]string{"ID", "NAME"}, [][]string{{"1", "a"}, {"2", "b"}})
	})
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 3)
	assert.Equal(t, "ID|NAME", lines[0])
	assert.Equal(t, "1|a", lines[1])
	assert.Equal(t, "2|b", lines[2])
}

func TestPrintTableHumanAligns(t *testing.T) {
	CurrentMode = ModeHuman
	out := capture(t, func() {
		PrintTable([]string{"ID", "NAME"}, [][]string{{"1", "alpha"}, {"200", "b"}})
	})
	// Longest ID cell is "200" (3 chars) so the header pads to width 3.
	assert.Contains(t, out, "ID  | NAME")
	assert.Contains(t, out, "1   | alpha")
}

func TestJSON(t *testing.T) {
	out := capture(t, func() {
		_ = JSON(map[string]any{"upserted": 3})
	})
	var got map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	assert.Equal(t, float64(3), got["upserted"])
}

func TestJSONRawReindents(t *testing.T) {
	out := capture(t, func() {
		JSONRaw([]byte(`{"query_id":"q1","results":[]}`))
	})
	assert.Contains(t, out, "\"query_id\": \"q1\"")
	// Valid JSON round-trips.
	var got map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	assert.Equal(t, "q1", got["query_id"])
}
