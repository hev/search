package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/hev/search/cli/internal/client"
	"github.com/hev/search/cli/internal/config"
)

// newClient builds a client from the resolved endpoint for this invocation.
func newClient() *client.Client {
	return client.New(config.Resolve(urlFlag))
}

// ctx is the request context for a single command invocation.
func ctx() context.Context { return context.Background() }

// confirmPrompt asks a y/N question on stdin, defaulting to no. In json
// mode there is no interactive prompt, so it returns false — callers must
// pass an explicit -y for destructive actions in non-interactive use.
func confirmPrompt(question string) bool {
	fmt.Fprintf(os.Stderr, "%s [y/N] ", question)
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	line = strings.TrimSpace(strings.ToLower(line))
	return line == "y" || line == "yes"
}

// idString renders a raw JSON row id (u64 or string) as a plain string.
func idString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var n json.Number
	if json.Unmarshal(raw, &n) == nil {
		return n.String()
	}
	return strings.TrimSpace(string(raw))
}

// idLiteral renders a row id as a DataFusion SQL literal for a /list or
// /delete filter. String ids are single-quoted (with quotes escaped);
// numeric ids are left bare.
func idLiteral(id string, stringType bool) string {
	if stringType {
		return "'" + strings.ReplaceAll(id, "'", "''") + "'"
	}
	return id
}

// parseIDArg coerces a CLI id argument to the right JSON type: a bare
// integer becomes a number, anything else stays a string.
func parseIDArg(s string) interface{} {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	return s
}

// attrString renders an attribute value compactly for a table cell.
func attrString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}
