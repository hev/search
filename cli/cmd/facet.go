package cmd

import (
	"fmt"
	"strings"

	"github.com/hev/search/cli/internal/client"
	"github.com/hev/search/cli/internal/output"
	"github.com/spf13/cobra"
)

var facetCmd = &cobra.Command{
	Use:   "facet -n NS --fields a,b",
	Short: "Compute facet counts over a filtered set",
	Long:  `Computes value counts for scalar fields via POST /ns/{ns}/facet.`,
	RunE:  runFacet,
}

var (
	facetNamespace string
	facetFields    []string
	facetFilter    string
	facetTop       int
)

func init() {
	facetCmd.Flags().StringVarP(&facetNamespace, "namespace", "n", "", "Namespace (required)")
	facetCmd.Flags().StringSliceVar(&facetFields, "fields", nil, "Comma-separated scalar fields to aggregate (required)")
	facetCmd.Flags().StringVar(&facetFilter, "filter", "", "DataFusion SQL predicate")
	facetCmd.Flags().IntVar(&facetTop, "top", 0, "Max buckets per field (0 = engine default)")
	_ = facetCmd.MarkFlagRequired("namespace")
	_ = facetCmd.MarkFlagRequired("fields")
	rootCmd.AddCommand(facetCmd)
}

func runFacet(cmd *cobra.Command, args []string) error {
	req := client.FacetRequest{Fields: facetFields}
	if facetFilter != "" {
		req.Filter = &facetFilter
	}
	if facetTop > 0 {
		req.Top = &facetTop
	}

	c := newClient()
	res, raw, err := c.Facet(ctx(), facetNamespace, req)
	if err != nil {
		return err
	}
	if output.IsJSON() {
		output.JSONRaw(raw)
		return nil
	}
	for i, f := range res.Facets {
		if i > 0 {
			fmt.Println()
		}
		title := f.Field
		if f.Truncated {
			title += " (truncated)"
		}
		output.Statusf("# %s", title)
		rows := make([][]string, 0, len(f.Buckets))
		for _, b := range f.Buckets {
			rows = append(rows, []string{facetValue(b.Value), fmt.Sprintf("%d", b.Count)})
		}
		output.PrintTable([]string{"VALUE", "COUNT"}, rows)
	}
	return nil
}

func facetValue(v interface{}) string {
	if v == nil {
		return "(null)"
	}
	return strings.TrimSpace(attrString(v))
}
