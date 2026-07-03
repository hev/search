package cmd

import (
	"github.com/spf13/cobra"
)

var compactCmd = &cobra.Command{
	Use:   "compact -n NS",
	Short: "Compact a namespace's data files (POST /ns/{ns}/compact)",
	RunE:  runCompact,
}

var (
	compactNamespace string
	compactWait      bool
)

func init() {
	compactCmd.Flags().StringVarP(&compactNamespace, "namespace", "n", "", "Namespace (required)")
	compactCmd.Flags().BoolVar(&compactWait, "wait", false, "Poll the operation until it completes")
	_ = compactCmd.MarkFlagRequired("namespace")
	rootCmd.AddCommand(compactCmd)
}

func runCompact(cmd *cobra.Command, args []string) error {
	c := newClient()
	op, raw, err := c.Compact(ctx(), compactNamespace)
	if err != nil {
		return err
	}
	return handleAccepted(c, op, raw, compactWait)
}
