// Command hev is a CLI for the hev search engine's internal REST API.
//
// hev search is a vector / FTS / hybrid search engine that runs behind
// hev layer. This binary is an operator/agent tool that speaks the
// engine's internal REST surface directly (default http://localhost:3000).
// It is not the inbound wire clients use — that is Layer's Turbopuffer-shaped
// API — it is the admin/debug path into the engine itself.
package main

import "github.com/hev/search/cli/cmd"

func main() {
	cmd.Execute()
}
