package cmd

import (
	"fmt"

	"github.com/hev/search/cli/internal/client"
	"github.com/hev/search/cli/internal/output"
	"github.com/spf13/cobra"
)

var getCmd = &cobra.Command{
	Use:   "get -n NS ID",
	Short: "Fetch a single row by id",
	Long: `Fetches one row by id via GET /ns/{ns}/list with filter "id = <literal>".

The id is quoted or left bare according to the namespace's id_type, which
is looked up from GET /ns/{ns} first. If that lookup fails, both a numeric
and a quoted match are attempted.`,
	Args: cobra.ExactArgs(1),
	RunE: runGet,
}

var getNamespace string

func init() {
	getCmd.Flags().StringVarP(&getNamespace, "namespace", "n", "", "Namespace (required)")
	_ = getCmd.MarkFlagRequired("namespace")
	rootCmd.AddCommand(getCmd)
}

func runGet(cmd *cobra.Command, args []string) error {
	c := newClient()
	id := args[0]

	// Prefer the namespace's declared id_type so the SQL literal is quoted
	// correctly; fall back to trying numeric-then-quoted if info is absent.
	stringType, known := lookupStringIDType(c, getNamespace)

	attempts := []bool{stringType}
	if !known {
		attempts = []bool{false, true}
	}

	var lastErr error
	for _, asString := range attempts {
		filter := fmt.Sprintf("id = %s", idLiteral(id, asString))
		page, raw, err := c.List(ctx(), getNamespace, client.ListParams{Limit: 1, Filter: filter})
		if err != nil {
			lastErr = err
			continue
		}
		if len(page.Rows) == 0 {
			continue
		}
		if output.IsJSON() {
			output.JSONRaw(raw)
			return nil
		}
		return printRow(page.Rows[0])
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("no row with id %q in %s", id, getNamespace)
}

// lookupStringIDType reports whether a namespace uses string ids. The
// second return is false when the namespace info could not be fetched.
func lookupStringIDType(c *client.Client, ns string) (bool, bool) {
	info, _, err := c.Info(ctx(), ns)
	if err != nil {
		return false, false
	}
	return info.IDType == "string", true
}

func printRow(r client.ListRow) error {
	pairs := [][2]string{
		{"id", idString(r.ID)},
		{"ingested_at_micros", fmt.Sprintf("%d", r.IngestedAtMicros)},
	}
	if r.Text != nil {
		pairs = append(pairs, [2]string{"text", *r.Text})
	}
	for k, v := range r.Attributes {
		pairs = append(pairs, [2]string{"attr." + k, attrString(v)})
	}
	if len(r.Vector) > 0 {
		pairs = append(pairs, [2]string{"vector_dim", fmt.Sprintf("%d", len(r.Vector))})
	}
	output.KeyVal(pairs)
	return nil
}
