package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hev/search/cli/internal/client"
)

// ctxbg is the request context for TUI-issued calls.
func ctxbg() context.Context { return context.Background() }

// statusBar renders the endpoint URL and active profile for the footer.
func statusBar(c *client.Client) string {
	ep := c.Endpoint()
	profile := ep.Profile
	if profile == "" {
		profile = "(none)"
	}
	return statusStyle.Render(fmt.Sprintf("%s • profile: %s", ep.URL, profile))
}

// sanitizeLine collapses whitespace/control chars to single spaces so a
// value renders on one row.
func sanitizeLine(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	prevSpace := false
	for _, r := range s {
		if r == '\r' || r == '\n' || r == '\t' || unicode.IsControl(r) || unicode.IsSpace(r) {
			if !prevSpace {
				b.WriteByte(' ')
				prevSpace = true
			}
			continue
		}
		b.WriteRune(r)
		prevSpace = false
	}
	return strings.TrimSpace(b.String())
}

// truncate cuts s to maxLen runes with an ellipsis, never splitting a rune.
func truncate(s string, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}
	if utf8.RuneCountInString(s) <= maxLen {
		return s
	}
	runes := []rune(s)
	if maxLen <= 3 {
		return string(runes[:maxLen])
	}
	return string(runes[:maxLen-3]) + "..."
}

// rowID renders a raw JSON id (u64 or string) as a plain string.
func rowID(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return strings.TrimSpace(string(raw))
}

// docPreview builds a one-line preview of a row, preferring the configured
// content field, then text, then attributes.
func docPreview(r client.ListRow, contentField string, maxLen int) string {
	if contentField != "" {
		if v, ok := r.Attributes[contentField]; ok {
			return truncate(sanitizeLine(valString(v)), maxLen)
		}
	}
	if r.Text != nil {
		return truncate(sanitizeLine(*r.Text), maxLen)
	}
	if len(r.Attributes) > 0 {
		b, _ := json.Marshal(r.Attributes)
		return truncate(sanitizeLine(string(b)), maxLen)
	}
	return ""
}

// resultPreview is docPreview for a query hit.
func resultPreview(r client.QueryResult, contentField string, maxLen int) string {
	if contentField != "" {
		if v, ok := r.Attributes[contentField]; ok {
			return truncate(sanitizeLine(valString(v)), maxLen)
		}
	}
	if r.Text != nil {
		return truncate(sanitizeLine(*r.Text), maxLen)
	}
	if len(r.Attributes) > 0 {
		b, _ := json.Marshal(r.Attributes)
		return truncate(sanitizeLine(string(b)), maxLen)
	}
	return ""
}

func valString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

// listRowJSON pretty-prints a list row without the (large) vector field.
func listRowJSON(r client.ListRow) string {
	m := map[string]interface{}{
		"id":                 rawOrString(r.ID),
		"ingested_at_micros": r.IngestedAtMicros,
	}
	if r.Text != nil {
		m["text"] = *r.Text
	}
	if len(r.Attributes) > 0 {
		m["attributes"] = r.Attributes
	}
	if len(r.Vector) > 0 {
		m["vector_dim"] = len(r.Vector)
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return rowID(r.ID)
	}
	return string(b)
}

// resultJSON pretty-prints a query hit without the vector field.
func resultJSON(r client.QueryResult) string {
	m := map[string]interface{}{
		"id":    rawOrString(r.ID),
		"score": r.Score,
	}
	if r.Text != nil {
		m["text"] = *r.Text
	}
	if r.IngestedAtMicros != nil {
		m["ingested_at_micros"] = *r.IngestedAtMicros
	}
	if len(r.Attributes) > 0 {
		m["attributes"] = r.Attributes
	}
	if len(r.Vector) > 0 {
		m["vector_dim"] = len(r.Vector)
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return rowID(r.ID)
	}
	return string(b)
}

func rawOrString(raw json.RawMessage) interface{} {
	var v interface{}
	if json.Unmarshal(raw, &v) == nil {
		return v
	}
	return string(raw)
}
