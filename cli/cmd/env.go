package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/hev/search/cli/internal/config"
	"github.com/hev/search/cli/internal/output"
	"github.com/spf13/cobra"
)

var activeMarkerStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("78"))

var envCmd = &cobra.Command{
	Use:   "env",
	Short: "Manage engine connection profiles (~/.hevsearch/config.toml)",
}

var envAddCmd = &cobra.Command{
	Use:   "add NAME",
	Short: "Add a profile (interactive)",
	Args:  cobra.ExactArgs(1),
	RunE:  runEnvAdd,
}

var envUseCmd = &cobra.Command{
	Use:   "use NAME",
	Short: "Switch the active profile",
	Args:  cobra.ExactArgs(1),
	RunE:  runEnvUse,
}

var envListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List profiles",
	RunE:    runEnvList,
}

var envRmCmd = &cobra.Command{
	Use:   "rm NAME",
	Short: "Remove a profile",
	Args:  cobra.ExactArgs(1),
	RunE:  runEnvRm,
}

var envShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show the active profile",
	RunE:  runEnvShow,
}

func init() {
	envCmd.AddCommand(envAddCmd, envUseCmd, envListCmd, envRmCmd, envShowCmd)
	rootCmd.AddCommand(envCmd)
}

func runEnvAdd(cmd *cobra.Command, args []string) error {
	name := args[0]
	reader := bufio.NewReader(os.Stdin)

	fmt.Fprintf(os.Stderr, "Base URL [%s]: ", config.DefaultURL)
	url, _ := reader.ReadString('\n')
	url = strings.TrimSpace(url)
	if url == "" {
		url = config.DefaultURL
	}

	if err := config.AddProfile(name, url); err != nil {
		return err
	}
	output.Statusf("Profile %q added.", name)
	if active, _, _ := config.GetActiveProfile(); active == name {
		output.Statusf("Set as active profile.")
	}
	return nil
}

func runEnvUse(cmd *cobra.Command, args []string) error {
	if err := config.SetActive(args[0]); err != nil {
		return err
	}
	output.Statusf("Switched to profile %q.", args[0])
	return nil
}

func runEnvList(cmd *cobra.Command, args []string) error {
	profiles := config.ListProfiles()
	if output.IsJSON() {
		list := make([]map[string]any, 0, len(profiles))
		for _, p := range profiles {
			list = append(list, map[string]any{
				"name":   p.Name,
				"url":    p.Config.URL,
				"active": p.IsActive,
			})
		}
		return output.JSON(map[string]any{"profiles": list})
	}
	if len(profiles) == 0 {
		output.Statusf("No profiles configured. Run 'hev env add <name>'.")
		return nil
	}
	headers := []string{"", "NAME", "URL"}
	var rows [][]string
	for _, p := range profiles {
		marker := ""
		if p.IsActive {
			if output.IsPlain() {
				marker = "*"
			} else {
				marker = activeMarkerStyle.Render("*")
			}
		}
		rows = append(rows, []string{
			marker,
			p.Name,
			p.Config.URL,
		})
	}
	output.PrintTable(headers, rows)
	return nil
}

func runEnvRm(cmd *cobra.Command, args []string) error {
	name := args[0]
	if !confirmPrompt(fmt.Sprintf("Remove profile %q?", name)) {
		output.Statusf("Aborted.")
		return nil
	}
	if err := config.RemoveProfile(name); err != nil {
		return err
	}
	output.Statusf("Profile %q removed.", name)
	return nil
}

func runEnvShow(cmd *cobra.Command, args []string) error {
	name, p, ok := config.GetActiveProfile()
	if !ok {
		if output.IsJSON() {
			return output.JSON(map[string]any{"active": nil})
		}
		output.Statusf("No active profile. Run 'hev env add <name>'.")
		return nil
	}
	if output.IsJSON() {
		return output.JSON(map[string]any{
			"active":        name,
			"url":           p.URL,
			"content_field": p.ContentField,
		})
	}
	pairs := [][2]string{
		{"active", name},
		{"url", p.URL},
	}
	if p.ContentField != "" {
		pairs = append(pairs, [2]string{"content_field", p.ContentField})
	}
	output.KeyVal(pairs)
	return nil
}
