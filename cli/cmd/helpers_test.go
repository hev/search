package cmd

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIDString(t *testing.T) {
	assert.Equal(t, "7", idString(json.RawMessage(`7`)))
	assert.Equal(t, "abc", idString(json.RawMessage(`"abc"`)))
	assert.Equal(t, "", idString(nil))
}

func TestIDLiteral(t *testing.T) {
	assert.Equal(t, "7", idLiteral("7", false))
	assert.Equal(t, "'abc'", idLiteral("abc", true))
	assert.Equal(t, "'o''brien'", idLiteral("o'brien", true), "single quotes should be doubled for SQL")
}

func TestParseIDArg(t *testing.T) {
	assert.Equal(t, int64(42), parseIDArg("42"))
	assert.Equal(t, "abc", parseIDArg("abc"))
	assert.Equal(t, "007x", parseIDArg("007x"))
}

func TestParseUpsertPayload(t *testing.T) {
	// Bare array of rows.
	req, err := parseUpsertPayload([]byte(`[{"id":1,"text":"hi"}]`))
	assert.NoError(t, err)
	assert.Len(t, req.Rows, 1)
	assert.Nil(t, req.DistanceMetric)

	// Full request body.
	req, err = parseUpsertPayload([]byte(`{"distance_metric":"cosine","rows":[{"id":2}]}`))
	assert.NoError(t, err)
	assert.Len(t, req.Rows, 1)
	if assert.NotNil(t, req.DistanceMetric) {
		assert.Equal(t, "cosine", *req.DistanceMetric)
	}

	_, err = parseUpsertPayload([]byte(`   `))
	assert.Error(t, err)
}

func TestParseFuzzy(t *testing.T) {
	f, err := parseFuzzy("auto")
	assert.NoError(t, err)
	assert.Equal(t, "auto", f.MaxEditDistance)

	f, err = parseFuzzy("2")
	assert.NoError(t, err)
	assert.Equal(t, 2, f.MaxEditDistance)

	_, err = parseFuzzy("5")
	assert.Error(t, err)
}
