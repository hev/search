package cmd

import (
	"github.com/hev/search/cli/internal/output"
	"github.com/spf13/cobra"
)

var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check engine liveness (GET /health)",
	RunE:  runHealth,
}

func init() {
	rootCmd.AddCommand(healthCmd)
}

func runHealth(cmd *cobra.Command, args []string) error {
	c := newClient()
	body, err := c.Health(ctx())
	if err != nil {
		return err
	}
	if output.IsJSON() {
		return output.JSON(map[string]any{"status": body, "url": c.Endpoint().URL})
	}
	output.Statusf("ok (%s)", c.Endpoint().URL)
	return nil
}
