package cmd

import (
	"fmt"

	"github.com/hev/search/cli/internal/client"
	"github.com/hev/search/cli/internal/output"
	"github.com/spf13/cobra"
)

var deleteCmd = &cobra.Command{
	Use:   "delete -n NS",
	Short: "Delete a namespace, or rows within it",
	Long: `With no selector, deletes the whole namespace (DELETE /ns/{ns}) after a
y/N confirmation (skip with -y).

With --ids or --filter (exactly one), deletes matching rows
(POST /ns/{ns}/delete).`,
	RunE: runDelete,
}

var (
	deleteNamespace string
	deleteIDs       []string
	deleteFilter    string
	deleteYes       bool
)

func init() {
	deleteCmd.Flags().StringVarP(&deleteNamespace, "namespace", "n", "", "Namespace (required)")
	deleteCmd.Flags().StringSliceVar(&deleteIDs, "ids", nil, "Comma-separated row ids to delete")
	deleteCmd.Flags().StringVar(&deleteFilter, "filter", "", "DataFusion SQL predicate selecting rows to delete")
	deleteCmd.Flags().BoolVarP(&deleteYes, "yes", "y", false, "Skip the confirmation prompt")
	_ = deleteCmd.MarkFlagRequired("namespace")
	rootCmd.AddCommand(deleteCmd)
}

func runDelete(cmd *cobra.Command, args []string) error {
	hasIDs := len(deleteIDs) > 0
	hasFilter := deleteFilter != ""
	if hasIDs && hasFilter {
		return fmt.Errorf("set exactly one of --ids or --filter, not both")
	}

	c := newClient()
	if hasIDs || hasFilter {
		return deleteRows(c, hasIDs)
	}
	return deleteNamespaceCmd(c)
}

func deleteRows(c *client.Client, hasIDs bool) error {
	req := client.DeleteRowsRequest{}
	if hasIDs {
		ids := make([]interface{}, 0, len(deleteIDs))
		for _, s := range deleteIDs {
			ids = append(ids, parseIDArg(s))
		}
		req.IDs = ids
	} else {
		req.Filter = &deleteFilter
	}

	res, raw, err := c.DeleteRows(ctx(), deleteNamespace, req)
	if err != nil {
		return err
	}
	if output.IsJSON() {
		output.JSONRaw(raw)
		return nil
	}
	output.Statusf("Deleted %d rows from %s.", res.Deleted, deleteNamespace)
	return nil
}

func deleteNamespaceCmd(c *client.Client) error {
	if !deleteYes {
		if !confirmPrompt(fmt.Sprintf("Delete the entire namespace %q and all its objects?", deleteNamespace)) {
			output.Statusf("Aborted.")
			return nil
		}
	}
	res, raw, err := c.DeleteNamespace(ctx(), deleteNamespace)
	if err != nil {
		return err
	}
	if output.IsJSON() {
		output.JSONRaw(raw)
		return nil
	}
	output.Statusf("Deleted namespace %s (%d objects removed).", deleteNamespace, res.ObjectsDeleted)
	return nil
}
