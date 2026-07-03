package cmd

import (
	"github.com/hev/search/cli/internal/client"
	"github.com/spf13/cobra"
)

var indexCmd = &cobra.Command{
	Use:   "index",
	Short: "Build indexes on a namespace",
}

var (
	indexNamespace  string
	indexPartitions uint32
	indexSubVectors uint32
	indexBits       uint32
	indexScalarCol  string
	indexWait       bool
)

var indexCreateCmd = &cobra.Command{
	Use:   "create -n NS",
	Short: "Build an IVF_PQ vector index (POST /ns/{ns}/index)",
	RunE:  runIndexCreate,
}

var indexFtsCmd = &cobra.Command{
	Use:   "fts -n NS",
	Short: "Build a BM25 full-text index (POST /ns/{ns}/fts-index)",
	RunE:  runIndexFts,
}

var indexScalarCmd = &cobra.Command{
	Use:   "scalar -n NS",
	Short: "Build a BTree scalar index (POST /ns/{ns}/scalar-index)",
	RunE:  runIndexScalar,
}

func init() {
	for _, c := range []*cobra.Command{indexCreateCmd, indexFtsCmd, indexScalarCmd} {
		c.Flags().StringVarP(&indexNamespace, "namespace", "n", "", "Namespace (required)")
		c.Flags().BoolVar(&indexWait, "wait", false, "Poll the operation until it completes")
		_ = c.MarkFlagRequired("namespace")
	}
	indexCreateCmd.Flags().Uint32Var(&indexPartitions, "partitions", 0, "Number of IVF partitions (0 = engine default)")
	indexCreateCmd.Flags().Uint32Var(&indexSubVectors, "sub-vectors", 0, "Number of PQ sub-vectors (0 = engine default)")
	indexCreateCmd.Flags().Uint32Var(&indexBits, "bits", 0, "PQ codebook bit width: 4 or 8 (0 = engine default)")
	indexScalarCmd.Flags().StringVar(&indexScalarCol, "column", "", "Column to index (default: _ingested_at)")

	indexCmd.AddCommand(indexCreateCmd, indexFtsCmd, indexScalarCmd)
	rootCmd.AddCommand(indexCmd)
}

func runIndexCreate(cmd *cobra.Command, args []string) error {
	req := client.IndexRequest{Kind: "ivf_pq"}
	if indexPartitions > 0 {
		req.NumPartitions = &indexPartitions
	}
	if indexSubVectors > 0 {
		req.NumSubVectors = &indexSubVectors
	}
	if indexBits > 0 {
		req.NumBits = &indexBits
	}
	c := newClient()
	op, raw, err := c.CreateIndex(ctx(), indexNamespace, req)
	if err != nil {
		return err
	}
	return handleAccepted(c, op, raw, indexWait)
}

func runIndexFts(cmd *cobra.Command, args []string) error {
	c := newClient()
	op, raw, err := c.CreateFtsIndex(ctx(), indexNamespace)
	if err != nil {
		return err
	}
	return handleAccepted(c, op, raw, indexWait)
}

func runIndexScalar(cmd *cobra.Command, args []string) error {
	var req *client.ScalarIndexRequest
	if indexScalarCol != "" {
		req = &client.ScalarIndexRequest{Column: indexScalarCol}
	}
	c := newClient()
	op, raw, err := c.CreateScalarIndex(ctx(), indexNamespace, req)
	if err != nil {
		return err
	}
	return handleAccepted(c, op, raw, indexWait)
}
