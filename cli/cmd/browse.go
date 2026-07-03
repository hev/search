package cmd

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/hev/search/cli/internal/client"
	"github.com/hev/search/cli/internal/config"
	"github.com/hev/search/cli/internal/tui"
	"github.com/spf13/cobra"
)

var browseCmd = &cobra.Command{
	Use:   "browse",
	Short: "Launch the interactive TUI browser",
	RunE: func(cmd *cobra.Command, args []string) error {
		return launchBrowser()
	},
}

func init() {
	rootCmd.AddCommand(browseCmd)
}

func launchBrowser() error {
	ep := config.Resolve(urlFlag)
	c := client.New(ep)
	p := tea.NewProgram(tui.New(c), tea.WithAltScreen())
	_, err := p.Run()
	return err
}
