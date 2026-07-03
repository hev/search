package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/hev/search/cli/internal/client"
	"github.com/hev/search/cli/internal/output"
	"github.com/spf13/cobra"
)

var upsertCmd = &cobra.Command{
	Use:   "upsert -n NS -f rows.json",
	Short: "Upsert rows into a namespace",
	Long: `Appends rows via POST /ns/{ns}/upsert.

The -f file may contain either a bare JSON array of rows or a full request
body ({"rows": [...], "distance_metric": "..."}). --distance-metric sets
the metric for a fresh namespace and overrides one in the file.

Use "-f -" to read the payload from stdin.`,
	RunE: runUpsert,
}

var (
	upsertNamespace string
	upsertFile      string
	upsertMetric    string
)

func init() {
	upsertCmd.Flags().StringVarP(&upsertNamespace, "namespace", "n", "", "Namespace (required)")
	upsertCmd.Flags().StringVarP(&upsertFile, "file", "f", "", "JSON file of rows or a full request body (required; - for stdin)")
	upsertCmd.Flags().StringVar(&upsertMetric, "distance-metric", "", "Distance metric for a fresh namespace: l2, cosine, or dot")
	_ = upsertCmd.MarkFlagRequired("namespace")
	_ = upsertCmd.MarkFlagRequired("file")
	rootCmd.AddCommand(upsertCmd)
}

func runUpsert(cmd *cobra.Command, args []string) error {
	raw, err := readFileOrStdin(upsertFile)
	if err != nil {
		return err
	}

	req, err := parseUpsertPayload(raw)
	if err != nil {
		return err
	}
	if upsertMetric != "" {
		m := upsertMetric
		req.DistanceMetric = &m
	}
	if len(req.Rows) == 0 {
		return fmt.Errorf("no rows to upsert")
	}

	c := newClient()
	res, rawResp, err := c.Upsert(ctx(), upsertNamespace, req)
	if err != nil {
		return err
	}
	if output.IsJSON() {
		output.JSONRaw(rawResp)
		return nil
	}
	output.Statusf("Upserted %d rows into %s.", res.Upserted, upsertNamespace)
	return nil
}

// parseUpsertPayload accepts either a bare rows array or a full request body.
func parseUpsertPayload(raw []byte) (client.UpsertRequest, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return client.UpsertRequest{}, fmt.Errorf("empty payload")
	}
	if trimmed[0] == '[' {
		var rows []client.UpsertRow
		if err := json.Unmarshal(trimmed, &rows); err != nil {
			return client.UpsertRequest{}, fmt.Errorf("parsing rows array: %w", err)
		}
		return client.UpsertRequest{Rows: rows}, nil
	}
	var req client.UpsertRequest
	if err := json.Unmarshal(trimmed, &req); err != nil {
		return client.UpsertRequest{}, fmt.Errorf("parsing request body: %w", err)
	}
	return req, nil
}

func readFileOrStdin(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	return b, nil
}
