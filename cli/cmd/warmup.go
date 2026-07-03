package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/hev/search/cli/internal/client"
	"github.com/hev/search/cli/internal/output"
	"github.com/spf13/cobra"
)

var warmupCmd = &cobra.Command{
	Use:   "warmup -n NS -f queries.json",
	Short: "Pre-populate the cache with queries (POST /ns/{ns}/warmup)",
	Long: `Enqueues a batch of queries to run through the cache-aside path.

The -f file may be a bare JSON array of query objects or a full request
body ({"queries": [...]}). Use "-f -" to read from stdin.`,
	RunE: runWarmup,
}

var (
	warmupNamespace string
	warmupFile      string
)

func init() {
	warmupCmd.Flags().StringVarP(&warmupNamespace, "namespace", "n", "", "Namespace (required)")
	warmupCmd.Flags().StringVarP(&warmupFile, "file", "f", "", "JSON file of query objects or a full request body (required; - for stdin)")
	_ = warmupCmd.MarkFlagRequired("namespace")
	_ = warmupCmd.MarkFlagRequired("file")
	rootCmd.AddCommand(warmupCmd)
}

func runWarmup(cmd *cobra.Command, args []string) error {
	raw, err := readFileOrStdin(warmupFile)
	if err != nil {
		return err
	}
	req, err := parseWarmupPayload(raw)
	if err != nil {
		return err
	}
	if len(req.Queries) == 0 {
		return fmt.Errorf("no queries to warm up")
	}

	c := newClient()
	res, rawResp, err := c.Warmup(ctx(), warmupNamespace, req)
	if err != nil {
		return err
	}
	if output.IsJSON() {
		output.JSONRaw(rawResp)
		return nil
	}
	output.Statusf("Queued %d warmup queries on %s (operation_id %s).", res.Queued, warmupNamespace, res.OperationID)
	return nil
}

func parseWarmupPayload(raw []byte) (client.WarmupRequest, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return client.WarmupRequest{}, fmt.Errorf("empty payload")
	}
	if trimmed[0] == '[' {
		var queries []json.RawMessage
		if err := json.Unmarshal(trimmed, &queries); err != nil {
			return client.WarmupRequest{}, fmt.Errorf("parsing queries array: %w", err)
		}
		return client.WarmupRequest{Queries: queries}, nil
	}
	var req client.WarmupRequest
	if err := json.Unmarshal(trimmed, &req); err != nil {
		return client.WarmupRequest{}, fmt.Errorf("parsing request body: %w", err)
	}
	return req, nil
}
