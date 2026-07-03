package cmd

import (
	"fmt"

	"github.com/hev/search/cli/internal/output"
	"github.com/spf13/cobra"
)

var opCmd = &cobra.Command{
	Use:   "op OP_ID",
	Short: "Show a background operation's status (GET /operations/{id})",
	Args:  cobra.ExactArgs(1),
	RunE:  runOp,
}

var opWait bool

func init() {
	opCmd.Flags().BoolVar(&opWait, "wait", false, "Poll until the operation reaches a terminal state")
	rootCmd.AddCommand(opCmd)
}

func runOp(cmd *cobra.Command, args []string) error {
	c := newClient()
	if opWait {
		return waitForOp(c, args[0])
	}
	rec, raw, err := c.GetOperation(ctx(), args[0])
	if err != nil {
		return err
	}
	if output.IsJSON() {
		output.JSONRaw(raw)
		return nil
	}
	pairs := [][2]string{
		{"operation_id", rec.OperationID},
		{"kind", rec.Kind},
		{"namespace", rec.Namespace},
		{"status", rec.Status},
	}
	if rec.Error != nil {
		pairs = append(pairs, [2]string{"error", *rec.Error})
	}
	output.KeyVal(pairs)
	if rec.Status == "failed" {
		msg := ""
		if rec.Error != nil {
			msg = ": " + *rec.Error
		}
		return fmt.Errorf("operation %s failed%s", rec.OperationID, msg)
	}
	return nil
}
