package cmd

import (
	"fmt"
	"os"

	"github.com/hev/search/cli/internal/output"
	"github.com/spf13/cobra"
)

// version is overridden at build time via
// -ldflags "-X github.com/hev/search/cli/cmd.version=x.y.z".
var version = "dev"

var (
	urlFlag        string
	outputModeFlag string
)

var rootCmd = &cobra.Command{
	Use:   "hev",
	Short: "CLI for the hev search engine",
	Long: `hev is an operator/agent CLI for the hev search engine's internal
REST API (vector / FTS / hybrid search on object storage).

It speaks the engine's internal REST surface directly (default
http://localhost:3000) — the admin/debug path, not the inbound wire that
clients use through hev layer. Run with no arguments to launch the TUI.

Output auto-detects: a terminal gets human-readable tables, a pipe gets
JSON. Force it with -o/--output human|plain|json.`,
	SilenceUsage:  true,
	SilenceErrors: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		mode, err := output.Resolve(outputModeFlag)
		if err != nil {
			return err
		}
		output.CurrentMode = mode
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return launchBrowser()
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&urlFlag, "url", "", "Engine base URL (overrides HEVSEARCH_URL and profile)")
	rootCmd.PersistentFlags().StringVarP(&outputModeFlag, "output", "o", "", "Output format: human, plain, or json (default: auto)")
	rootCmd.Version = version
}

// Execute runs the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}
