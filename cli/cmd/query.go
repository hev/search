package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/hev/search/cli/internal/client"
	"github.com/hev/search/cli/internal/output"
	"github.com/spf13/cobra"
)

var queryCmd = &cobra.Command{
	Use:   "query -n NS [QUERY_TEXT]",
	Short: "Run a vector / FTS / hybrid query",
	Long: `Runs a search via POST /ns/{ns}/query.

Query mode follows the fields set:
  - QUERY_TEXT only            → FTS (BM25)
  - --vector / --vector-file   → vector ANN
  - both                       → hybrid (RRF)

Vectors are omitted from results by default to keep output readable; pass
--with-vectors to include them.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runQuery,
}

var (
	queryNamespace   string
	queryK           int
	queryFilter      string
	queryNprobes     int
	queryFuzzy       string
	queryVector      string
	queryVectorFile  string
	queryWithVectors bool
	queryNoVector    bool
)

func init() {
	queryCmd.Flags().StringVarP(&queryNamespace, "namespace", "n", "", "Namespace (required)")
	queryCmd.Flags().IntVarP(&queryK, "k", "k", 10, "Number of results")
	queryCmd.Flags().StringVar(&queryFilter, "filter", "", "DataFusion SQL prefilter")
	queryCmd.Flags().IntVar(&queryNprobes, "nprobes", 0, "IVF partitions to probe (0 = engine default)")
	queryCmd.Flags().StringVar(&queryFuzzy, "fuzzy", "", "Fuzzy FTS max edit distance: 0, 1, 2, or auto")
	queryCmd.Flags().StringVar(&queryVector, "vector", "", "Query vector as a JSON array, e.g. '[0.1,0.2]'")
	queryCmd.Flags().StringVar(&queryVectorFile, "vector-file", "", "File containing the query vector as a JSON array")
	queryCmd.Flags().BoolVar(&queryWithVectors, "with-vectors", false, "Include stored vectors in results")
	queryCmd.Flags().BoolVar(&queryNoVector, "no-vector", false, "Exclude stored vectors from results (default)")
	_ = queryCmd.MarkFlagRequired("namespace")
	rootCmd.AddCommand(queryCmd)
}

func runQuery(cmd *cobra.Command, args []string) error {
	req := client.QueryRequest{
		K:             queryK,
		IncludeVector: queryWithVectors && !queryNoVector,
	}

	if len(args) == 1 && args[0] != "" {
		text := args[0]
		req.Text = &text
	}

	if queryVector != "" || queryVectorFile != "" {
		vec, err := loadVector()
		if err != nil {
			return err
		}
		req.Vector = vec
	}

	if req.Text == nil && len(req.Vector) == 0 {
		return fmt.Errorf("provide QUERY_TEXT, --vector, or --vector-file")
	}

	if queryNprobes > 0 {
		req.Nprobes = &queryNprobes
	}
	if queryFilter != "" {
		req.Filter = &queryFilter
	}
	if queryFuzzy != "" {
		fz, err := parseFuzzy(queryFuzzy)
		if err != nil {
			return err
		}
		req.Fuzzy = fz
	}

	c := newClient()
	res, raw, err := c.Query(ctx(), queryNamespace, req)
	if err != nil {
		return err
	}
	if output.IsJSON() {
		output.JSONRaw(raw)
		return nil
	}
	if len(res.Results) == 0 {
		output.Statusf("No results (query_id %s).", res.QueryID)
		return nil
	}
	headers := []string{"ID", "SCORE", "TEXT/ATTRS"}
	rows := make([][]string, 0, len(res.Results))
	for _, r := range res.Results {
		preview := ""
		if r.Text != nil {
			preview = *r.Text
		} else if len(r.Attributes) > 0 {
			preview = attrString(r.Attributes)
		}
		rows = append(rows, []string{
			idString(r.ID),
			strconv.FormatFloat(float64(r.Score), 'f', 4, 32),
			truncateCell(preview, 60),
		})
	}
	output.PrintTable(headers, rows)
	output.Statusf("query_id: %s", res.QueryID)
	return nil
}

func loadVector() ([]float32, error) {
	raw := []byte(queryVector)
	if queryVectorFile != "" {
		b, err := os.ReadFile(queryVectorFile)
		if err != nil {
			return nil, fmt.Errorf("reading vector file: %w", err)
		}
		raw = b
	}
	var vec []float32
	if err := json.Unmarshal(raw, &vec); err != nil {
		return nil, fmt.Errorf("vector must be a JSON array of numbers: %w", err)
	}
	if len(vec) == 0 {
		return nil, fmt.Errorf("vector is empty")
	}
	return vec, nil
}

func parseFuzzy(s string) (*client.FuzzyRequest, error) {
	switch s {
	case "auto":
		return &client.FuzzyRequest{MaxEditDistance: "auto"}, nil
	case "0", "1", "2":
		n, _ := strconv.Atoi(s)
		return &client.FuzzyRequest{MaxEditDistance: n}, nil
	default:
		return nil, fmt.Errorf("--fuzzy must be 0, 1, 2, or auto (got %q)", s)
	}
}
