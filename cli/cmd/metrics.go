package cmd

import (
	"fmt"
	"strconv"

	"github.com/hev/search/cli/internal/metrics"
	"github.com/hev/search/cli/internal/output"
	"github.com/spf13/cobra"
)

var metricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "Show engine metrics (GET /metrics)",
	Long: `Fetches the Prometheus metrics endpoint.

By default a curated subset is shown (cache, S3, query, index, and
compaction counters). --raw dumps the whole exposition unchanged; --grep
filters to metric names with the given prefix.`,
	RunE: runMetrics,
}

var (
	metricsRaw  bool
	metricsGrep string
)

func init() {
	metricsCmd.Flags().BoolVar(&metricsRaw, "raw", false, "Dump the raw Prometheus text")
	metricsCmd.Flags().StringVar(&metricsGrep, "grep", "", "Show only metrics whose name starts with this prefix")
	rootCmd.AddCommand(metricsCmd)
}

func runMetrics(cmd *cobra.Command, args []string) error {
	c := newClient()
	text, err := c.Metrics(ctx())
	if err != nil {
		return err
	}

	if metricsRaw && metricsGrep == "" {
		if output.IsJSON() {
			return output.JSON(map[string]any{"metrics": metrics.Parse(text)})
		}
		fmt.Print(text)
		return nil
	}

	samples := metrics.Parse(text)
	if metricsGrep != "" {
		samples = metrics.FilterPrefix(samples, metricsGrep)
	} else if !metricsRaw {
		samples = metrics.Filter(samples, metrics.CuratedSubstrings)
	}

	if output.IsJSON() {
		return output.JSON(map[string]any{"metrics": samples})
	}
	if len(samples) == 0 {
		output.Statusf("No matching metrics.")
		return nil
	}
	rows := make([][]string, 0, len(samples))
	for _, s := range samples {
		rows = append(rows, []string{s.Name + s.Labels, strconv.FormatFloat(s.Value, 'g', -1, 64)})
	}
	output.PrintTable([]string{"METRIC", "VALUE"}, rows)
	return nil
}
