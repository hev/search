# hev — the hev search engine CLI

`hev` is an operator/agent CLI for the **hev search** engine — the
proprietary vector / FTS / hybrid search engine that runs behind hev layer.
It speaks the engine's **internal REST API** directly (default
`http://localhost:3000`).

This is the admin/debug path into the engine itself, not the inbound wire
that application clients use (that is Layer's Turbopuffer-shaped API). The
engine is a trusted internal service; Layer owns authentication and
authorization at the edge.

## Install

```sh
make install        # builds and installs `hev` to $GOBIN (or ~/go/bin)
# or
go build -o hev .
```

Requires Go 1.25+.

## Connecting to an engine

Resolution precedence for the base URL:

```
--url flag  >  HEVSEARCH_URL env  >  active profile  >  http://localhost:3000
```

Profiles live in `~/.hevsearch/config.toml` (written `0600`). Manage them
with `hev env`:

```sh
hev env add local          # interactive: base URL
hev env use staging        # switch active profile
hev env list               # show configured profiles
hev env show               # show the active profile
hev env rm staging         # remove a profile
```

## Output modes

A global `-o/--output` flag selects `human`, `plain`, or `json`. When it is
omitted the mode auto-detects: a **terminal gets human** (aligned, styled
tables) and a **pipe gets json**. This makes the CLI agent-friendly by
default — piped or captured output is always machine-readable JSON, one
object per invocation, with field names matching the engine's REST API.

- `human` — aligned, colorized tables for interactive use.
- `plain` — pipe-delimited tables (`a|b|c`) for `grep`/`awk`.
- `json`  — the engine's response passed through (augmented where useful).

Status lines (e.g. `next_cursor:` hints, "Upserted N rows") are written to
**stderr** so they never pollute machine output on stdout.

## Commands

### Browse (TUI)

```sh
hev                 # launch the interactive browser
hev browse          # same
```

The TUI is a view stack: **namespaces → documents → preview → full JSON**.

- Namespaces list (from `GET /ns`) enriches visible rows lazily with row
  counts, dimension, and index flags (`V`ector / `F`TS / `S`calar).
- `enter` drills in, `esc`/`q` goes back, `i` opens the namespace info panel.
- In the documents view, `/` opens an FTS prompt that POSTs a text query and
  shows scored hits; scrolling to the tail pages the listing with the cursor.
- `y` copies the document JSON to the clipboard.
- Navigation: `j/k` (or arrows), `g/G` top/bottom, `r` refresh, `ctrl+c` quit.
- A status bar shows the base URL and active profile.

### Namespaces & rows

```sh
hev ls                              # list namespaces (GET /ns)
hev ls -n docs --limit 20           # list rows (GET /ns/docs/list)
hev ls -n docs --filter "year > 2020" --order asc --cursor <cursor>
hev info docs                       # namespace metadata (GET /ns/docs)
hev get -n docs 42                  # fetch one row by id
```

`hev ls -n NS` prints `next_cursor` (to stderr) so an agent can paginate by
feeding it back via `--cursor`. `hev get` looks up the namespace's `id_type`
to quote the id correctly, falling back to trying numeric then quoted.

### Query

```sh
hev query -n docs "vector databases"            # FTS (BM25)
hev query -n docs "vector databases" --fuzzy 1  # fuzzy FTS
hev query -n docs --vector '[0.1,0.2,0.3]' -k 5 # vector ANN
hev query -n docs "hybrid" --vector-file q.json # hybrid (vector + text)
hev query -n docs "x" --filter "year > 2020" --nprobes 40 --with-vectors
```

Mode follows the fields set: text only → FTS, vector only → ANN, both →
hybrid (RRF). Vectors are **excluded from results by default** to keep output
readable; pass `--with-vectors` to include them.

### Facets

```sh
hev facet -n docs --fields section,year --filter "year > 2020" --top 20
```

### Upsert

```sh
hev upsert -n docs -f rows.json
hev upsert -n docs -f rows.json --distance-metric cosine
cat rows.json | hev upsert -n docs -f -
```

The file may be a bare JSON array of rows or a full request body
(`{"rows": [...], "distance_metric": "..."}`).

### Delete (admin)

```sh
hev delete -n docs                    # whole namespace (confirms y/N)
hev delete -n docs -y                 # skip confirmation
hev delete -n docs --ids 1,2,3        # delete rows by id
hev delete -n docs --filter "year < 2000"
```

### Indexes & compaction (admin)

```sh
hev index create -n docs --partitions 256 --sub-vectors 16 --bits 8
hev index fts -n docs
hev index scalar -n docs --column id
hev compact -n docs
```

All start a background operation and print its `operation_id`. Add `--wait`
to poll `GET /operations/{id}` until the operation succeeds or fails.

### Operations

```sh
hev op OP_ID           # show status
hev op OP_ID --wait    # poll to a terminal state
```

A `failed` operation exits non-zero with the engine's error message.

### Warmup

```sh
hev warmup -n docs -f queries.json
```

The file is a JSON array of query objects (or `{"queries": [...]}`). Queries
are sent through unchanged so the cache keys match the real queries you warm.

### Health & metrics

```sh
hev health
hev metrics                       # curated subset (cache, S3, query, index)
hev metrics --grep hevsearch_cache
hev metrics --raw                 # full Prometheus exposition
```

## JSON mode for agents

Every command emits meaningful JSON in `json` mode (forced with `-o json`,
or automatic when stdout is not a TTY). The payload is the engine's own
response passed through, so field names are stable and match the REST API
(`crates/hevsearch-api/src/handlers.rs`). Errors from the engine are surfaced
as `HTTP <status>: <message>` on stderr with a non-zero exit; a down engine
reports `engine unreachable at <url>`.

## Development

```sh
make build      # build ./hev
make test       # go test ./... -race
make vet        # go vet ./...
make fmt        # gofmt -w .
```

Layout:

```
main.go                 entrypoint
cmd/                    one file per cobra command
internal/client/        hand-written REST client + wire types
internal/config/        TOML profiles + endpoint resolution
internal/output/        human / plain / json rendering
internal/metrics/       Prometheus text parsing
internal/tui/           Bubble Tea view-stack browser
```
