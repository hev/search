package cmd

import (
	"fmt"

	"github.com/hev/search/cli/internal/output"
	"github.com/spf13/cobra"
)

var infoCmd = &cobra.Command{
	Use:   "info NS",
	Short: "Show namespace metadata (GET /ns/{ns})",
	Args:  cobra.ExactArgs(1),
	RunE:  runInfo,
}

func init() {
	rootCmd.AddCommand(infoCmd)
}

func runInfo(cmd *cobra.Command, args []string) error {
	c := newClient()
	info, raw, err := c.Info(ctx(), args[0])
	if err != nil {
		return err
	}
	if output.IsJSON() {
		output.JSONRaw(raw)
		return nil
	}
	output.KeyVal([][2]string{
		{"namespace", info.Namespace},
		{"kind", info.Kind},
		{"vector_dim", fmt.Sprintf("%d", info.VectorDim)},
		{"id_type", info.IDType},
		{"distance_metric", info.DistanceMetric},
		{"row_count", fmt.Sprintf("%d", info.RowCount)},
		{"fragment_count", fmt.Sprintf("%d", info.FragmentCount)},
		{"has_vector_index", fmt.Sprintf("%t", info.HasVectorIndex)},
		{"has_fts_index", fmt.Sprintf("%t", info.HasFtsIndex)},
		{"has_scalar_index", fmt.Sprintf("%t", info.HasScalarIndex)},
		{"table_version", fmt.Sprintf("%d", info.TableVersion)},
	})
	return nil
}
