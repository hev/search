package cmd

import (
	"fmt"

	"github.com/hev/search/cli/internal/client"
	"github.com/hev/search/cli/internal/output"
	"github.com/spf13/cobra"
)

var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List namespaces, or rows within a namespace",
	Long: `With no flags, lists namespaces (GET /ns).

With -n/--namespace, lists rows in that namespace (GET /ns/{ns}/list),
paginated by _ingested_at. The next_cursor is printed so agents can
page through with --cursor.`,
	RunE: runLs,
}

var (
	lsNamespace string
	lsLimit     int
	lsFilter    string
	lsOrder     string
	lsCursor    string
)

func init() {
	lsCmd.Flags().StringVarP(&lsNamespace, "namespace", "n", "", "List rows in this namespace")
	lsCmd.Flags().IntVar(&lsLimit, "limit", 0, "Max rows to return (list mode)")
	lsCmd.Flags().StringVar(&lsFilter, "filter", "", "DataFusion SQL predicate (list mode)")
	lsCmd.Flags().StringVar(&lsOrder, "order", "", "Sort order: asc or desc (list mode)")
	lsCmd.Flags().StringVar(&lsCursor, "cursor", "", "Pagination cursor from a previous page (list mode)")
	rootCmd.AddCommand(lsCmd)
}

func runLs(cmd *cobra.Command, args []string) error {
	c := newClient()
	if lsNamespace != "" {
		return listRows(c)
	}
	return listNamespaces(c)
}

func listNamespaces(c *client.Client) error {
	nsList, raw, err := c.ListNamespaces(ctx())
	if err != nil {
		return err
	}
	if output.IsJSON() {
		output.JSONRaw(raw)
		return nil
	}
	if len(nsList.Namespaces) == 0 {
		output.Statusf("No namespaces found.")
		return nil
	}
	rows := make([][]string, 0, len(nsList.Namespaces))
	for _, n := range nsList.Namespaces {
		rows = append(rows, []string{n})
	}
	output.PrintTable([]string{"NAMESPACE"}, rows)
	return nil
}

func listRows(c *client.Client) error {
	page, raw, err := c.List(ctx(), lsNamespace, client.ListParams{
		Order:  lsOrder,
		Limit:  lsLimit,
		Cursor: lsCursor,
		Filter: lsFilter,
	})
	if err != nil {
		return err
	}
	if output.IsJSON() {
		output.JSONRaw(raw)
		return nil
	}
	if len(page.Rows) == 0 {
		output.Statusf("No rows in %s.", lsNamespace)
		return nil
	}
	headers := []string{"ID", "INGESTED_AT_MICROS", "TEXT/ATTRS"}
	rows := make([][]string, 0, len(page.Rows))
	for _, r := range page.Rows {
		preview := ""
		if r.Text != nil {
			preview = *r.Text
		} else if len(r.Attributes) > 0 {
			preview = attrString(r.Attributes)
		}
		rows = append(rows, []string{
			idString(r.ID),
			fmt.Sprintf("%d", r.IngestedAtMicros),
			truncateCell(preview, 60),
		})
	}
	output.PrintTable(headers, rows)
	if page.NextCursor != nil && *page.NextCursor != "" {
		output.Statusf("next_cursor: %s", *page.NextCursor)
	}
	return nil
}

func truncateCell(s string, max int) string {
	s = collapseWS(s)
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	if max <= 3 {
		return string(r[:max])
	}
	return string(r[:max-3]) + "..."
}

func collapseWS(s string) string {
	out := make([]rune, 0, len(s))
	space := false
	for _, r := range s {
		if r == '\n' || r == '\r' || r == '\t' {
			r = ' '
		}
		if r == ' ' {
			if space {
				continue
			}
			space = true
		} else {
			space = false
		}
		out = append(out, r)
	}
	return string(out)
}
