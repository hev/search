package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/hev/search/cli/internal/client"
)

const docsPageSize = 50

type docsLoadedMsg struct {
	rows       []client.ListRow
	nextCursor string
	appending  bool
}
type docsErrMsg struct{ err error }
type searchResultsMsg struct {
	query   string
	results []client.QueryResult
}
type searchErrMsg struct {
	query string
	err   error
}

type documentsModel struct {
	namespace    string
	contentField string

	rows        []client.ListRow
	nextCursor  string
	loadingMore bool

	results      []client.QueryResult
	searchActive bool

	cursor  int
	loading bool
	err     error

	searching     bool
	searchQuery   string
	searchLoading bool
	searchErr     error
}

func newDocumentsModel(namespace, contentField string) documentsModel {
	return documentsModel{namespace: namespace, contentField: contentField, loading: true}
}

func (m documentsModel) init(c *client.Client) tea.Cmd {
	ns := m.namespace
	return func() tea.Msg {
		page, _, err := c.List(ctxbg(), ns, client.ListParams{Limit: docsPageSize})
		if err != nil {
			return docsErrMsg{err: err}
		}
		next := ""
		if page.NextCursor != nil {
			next = *page.NextCursor
		}
		return docsLoadedMsg{rows: page.Rows, nextCursor: next}
	}
}

func (m documentsModel) loadMore(c *client.Client) tea.Cmd {
	ns := m.namespace
	cursor := m.nextCursor
	return func() tea.Msg {
		page, _, err := c.List(ctxbg(), ns, client.ListParams{Limit: docsPageSize, Cursor: cursor})
		if err != nil {
			return docsErrMsg{err: err}
		}
		next := ""
		if page.NextCursor != nil {
			next = *page.NextCursor
		}
		return docsLoadedMsg{rows: page.Rows, nextCursor: next, appending: true}
	}
}

func (m documentsModel) runSearch(c *client.Client, query string) tea.Cmd {
	ns := m.namespace
	return func() tea.Msg {
		text := query
		res, _, err := c.Query(ctxbg(), ns, client.QueryRequest{K: docsPageSize, Text: &text, IncludeVector: false})
		if err != nil {
			return searchErrMsg{query: query, err: err}
		}
		return searchResultsMsg{query: query, results: res.Results}
	}
}

// selectedJSON returns the pretty JSON and id of the selected row/result.
func (m documentsModel) selectedJSON() (string, string, bool) {
	if m.searchActive {
		if m.cursor < 0 || m.cursor >= len(m.results) {
			return "", "", false
		}
		r := m.results[m.cursor]
		return resultJSON(r), rowID(r.ID), true
	}
	if m.cursor < 0 || m.cursor >= len(m.rows) {
		return "", "", false
	}
	r := m.rows[m.cursor]
	return listRowJSON(r), rowID(r.ID), true
}

func (m documentsModel) count() int {
	if m.searchActive {
		return len(m.results)
	}
	return len(m.rows)
}

func (m documentsModel) update(msg tea.Msg, c *client.Client) (documentsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case docsLoadedMsg:
		if msg.appending {
			m.rows = append(m.rows, msg.rows...)
		} else {
			m.rows = msg.rows
			m.cursor = 0
		}
		m.nextCursor = msg.nextCursor
		m.loading = false
		m.loadingMore = false
		return m, nil
	case docsErrMsg:
		m.err = msg.err
		m.loading = false
		m.loadingMore = false
		return m, nil
	case searchResultsMsg:
		if msg.query != m.searchQuery {
			return m, nil
		}
		m.results = msg.results
		m.cursor = 0
		m.searchActive = true
		m.searchLoading = false
		m.searchErr = nil
		return m, nil
	case searchErrMsg:
		if msg.query != m.searchQuery {
			return m, nil
		}
		m.searchErr = msg.err
		m.searchLoading = false
		return m, nil
	case tea.KeyMsg:
		if m.searching {
			return m.handleSearchInput(msg, c)
		}
		if m.searchActive && msg.String() == "esc" {
			m.searchActive = false
			m.results = nil
			m.searchQuery = ""
			m.searchErr = nil
			m.cursor = 0
			return m, nil
		}
		switch {
		case msg.String() == "j" || msg.String() == "down":
			if m.cursor < m.count()-1 {
				m.cursor++
			}
			// Load the next page when scrolling into the tail.
			if !m.searchActive && !m.loadingMore && m.nextCursor != "" && m.cursor >= len(m.rows)-1 {
				m.loadingMore = true
				return m, m.loadMore(c)
			}
		case msg.String() == "k" || msg.String() == "up":
			if m.cursor > 0 {
				m.cursor--
			}
		case msg.String() == "G":
			m.cursor = max(0, m.count()-1)
		case msg.String() == "g":
			m.cursor = 0
		case msg.String() == "/":
			m.searching = true
			m.searchQuery = ""
			m.searchErr = nil
		}
	}
	return m, nil
}

func (m documentsModel) handleSearchInput(msg tea.KeyMsg, c *client.Client) (documentsModel, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.searching = false
		if !m.searchActive {
			m.searchQuery = ""
		}
		return m, nil
	case "enter":
		q := strings.TrimSpace(m.searchQuery)
		if q == "" {
			m.searching = false
			return m, nil
		}
		m.searching = false
		m.searchLoading = true
		m.searchErr = nil
		m.searchQuery = q
		return m, m.runSearch(c, q)
	case "backspace":
		if len(m.searchQuery) > 0 {
			r := []rune(m.searchQuery)
			m.searchQuery = string(r[:len(r)-1])
		}
		return m, nil
	default:
		if len([]rune(msg.String())) == 1 {
			m.searchQuery += msg.String()
		}
		return m, nil
	}
}

func (m documentsModel) view(width, height int) string {
	if m.loading {
		return loadingStyle.Render(fmt.Sprintf("Loading documents from %s...", m.namespace))
	}
	if m.err != nil {
		return errorStyle.Render(fmt.Sprintf("Error: %s", m.err)) + "\n\n" +
			helpStyle.Render("esc back • q quit")
	}

	var b strings.Builder
	title := fmt.Sprintf(" %s (%d docs) ", m.namespace, len(m.rows))
	if m.searchActive {
		title = fmt.Sprintf(" %s — FTS %q (%d hits) ", m.namespace, m.searchQuery, len(m.results))
	}
	b.WriteString(headerStyle.Render(title))
	b.WriteString("\n\n")

	idW := 28
	if width > 90 {
		idW = 36
	}
	scoreW := 0
	if m.searchActive {
		scoreW = 8
	}
	previewW := width - idW - scoreW - 6
	if previewW < 20 {
		previewW = 40
	}

	contentsLabel := "Contents"
	if m.contentField != "" {
		contentsLabel = m.contentField
	}
	var header string
	if m.searchActive {
		header = fmt.Sprintf("  %-*s %*s %s", idW, "ID", scoreW, "Score", contentsLabel)
	} else {
		header = fmt.Sprintf("  %-*s %s", idW, "ID", contentsLabel)
	}
	b.WriteString(columnHeaderStyle.Render(header))
	b.WriteString("\n")

	reserved := 7
	if m.searching || m.searchErr != nil || m.searchLoading {
		reserved++
	}
	maxRows := height - reserved
	if maxRows < 1 {
		maxRows = 1
	}
	offset := 0
	if m.cursor >= maxRows {
		offset = m.cursor - maxRows + 1
	}

	n := m.count()
	if n == 0 {
		msg := "documents"
		if m.searchActive {
			msg = "matches"
		}
		b.WriteString(helpStyle.Render(fmt.Sprintf("  (no %s)", msg)))
		b.WriteString("\n")
	}

	for i := offset; i < n && i < offset+maxRows; i++ {
		var line string
		if m.searchActive {
			r := m.results[i]
			line = fmt.Sprintf("  %-*s %*.4f %s", idW, truncate(rowID(r.ID), idW), scoreW, r.Score, resultPreview(r, m.contentField, previewW))
		} else {
			r := m.rows[i]
			line = fmt.Sprintf("  %-*s %s", idW, truncate(rowID(r.ID), idW), docPreview(r, m.contentField, previewW))
		}
		if i == m.cursor {
			b.WriteString(selectedStyle.Render("▸ " + line[2:]))
		} else {
			b.WriteString(normalStyle.Render(line))
		}
		b.WriteString("\n")
	}

	b.WriteString("\n")
	switch {
	case m.searching:
		b.WriteString(statusStyle.Render(fmt.Sprintf("FTS: %s█", m.searchQuery)))
		b.WriteString("\n")
	case m.searchLoading:
		b.WriteString(loadingStyle.Render(fmt.Sprintf("Searching for %q...", m.searchQuery)))
		b.WriteString("\n")
	case m.searchErr != nil:
		b.WriteString(errorStyle.Render(fmt.Sprintf("Search error: %s", m.searchErr)))
		b.WriteString("\n")
	}

	if m.searching {
		b.WriteString(helpStyle.Render("type query • enter run • esc cancel"))
	} else if m.searchActive {
		b.WriteString(helpStyle.Render("↑/k ↓/j • enter preview • / new search • esc clear • i info • q back"))
	} else {
		b.WriteString(helpStyle.Render("↑/k ↓/j • enter preview • / FTS search • i info • esc back"))
	}
	return b.String()
}
