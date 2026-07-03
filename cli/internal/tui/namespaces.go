package tui

import (
	"fmt"
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/hev/search/cli/internal/client"
)

type nsListMsg struct{ names []string }
type nsInfoMsg struct {
	name string
	info *client.NamespaceInfo
}
type nsErrMsg struct{ err error }

type namespacesModel struct {
	names     []string
	info      map[string]*client.NamespaceInfo
	requested map[string]bool
	filtered  []int
	cursor    int
	loading   bool
	err       error
	filter    string
	filtering bool
}

func newNamespacesModel() namespacesModel {
	return namespacesModel{
		loading:   true,
		info:      make(map[string]*client.NamespaceInfo),
		requested: make(map[string]bool),
	}
}

func (m namespacesModel) init(c *client.Client) tea.Cmd {
	return func() tea.Msg {
		list, _, err := c.ListNamespaces(ctxbg())
		if err != nil {
			return nsErrMsg{err: err}
		}
		names := append([]string(nil), list.Namespaces...)
		sort.Strings(names)
		return nsListMsg{names: names}
	}
}

func fetchInfoCmd(c *client.Client, name string) tea.Cmd {
	return func() tea.Msg {
		info, _, err := c.Info(ctxbg(), name)
		if err != nil {
			return nsInfoMsg{name: name, info: nil}
		}
		return nsInfoMsg{name: name, info: &info}
	}
}

// enrichVisible requests info for the currently visible, not-yet-requested
// namespaces so the list fills in lazily as the operator scrolls.
func (m *namespacesModel) enrichVisible(c *client.Client, height int) tea.Cmd {
	visible := m.visibleIndices()
	maxRows := height - 6
	if maxRows < 1 {
		maxRows = 1
	}
	offset := 0
	if m.cursor >= maxRows {
		offset = m.cursor - maxRows + 1
	}
	var cmds []tea.Cmd
	for i := offset; i < len(visible) && i < offset+maxRows; i++ {
		name := m.names[visible[i]]
		if !m.requested[name] {
			m.requested[name] = true
			cmds = append(cmds, fetchInfoCmd(c, name))
		}
	}
	if len(cmds) == 0 {
		return nil
	}
	return tea.Batch(cmds...)
}

func (m namespacesModel) selected() string {
	visible := m.visibleIndices()
	if m.cursor < 0 || m.cursor >= len(visible) {
		return ""
	}
	return m.names[visible[m.cursor]]
}

func (m namespacesModel) visibleIndices() []int {
	if m.filter != "" {
		return m.filtered
	}
	idx := make([]int, len(m.names))
	for i := range m.names {
		idx[i] = i
	}
	return idx
}

func (m *namespacesModel) applyFilter() {
	if m.filter == "" {
		m.filtered = nil
		return
	}
	m.filtered = nil
	lower := strings.ToLower(m.filter)
	for i, n := range m.names {
		if strings.Contains(strings.ToLower(n), lower) {
			m.filtered = append(m.filtered, i)
		}
	}
	if m.cursor >= len(m.visibleIndices()) {
		m.cursor = max(0, len(m.visibleIndices())-1)
	}
}

func (m namespacesModel) update(msg tea.Msg, c *client.Client) (namespacesModel, tea.Cmd) {
	switch msg := msg.(type) {
	case nsListMsg:
		m.names = msg.names
		m.loading = false
		return m, m.enrichVisible(c, 24)
	case nsInfoMsg:
		if msg.info != nil {
			m.info[msg.name] = msg.info
		}
		return m, nil
	case nsErrMsg:
		m.err = msg.err
		m.loading = false
		return m, nil
	case tea.KeyMsg:
		if m.filtering {
			switch msg.String() {
			case "esc":
				m.filtering = false
				m.filter = ""
				m.applyFilter()
			case "enter":
				m.filtering = false
			case "backspace":
				if len(m.filter) > 0 {
					m.filter = m.filter[:len(m.filter)-1]
					m.applyFilter()
				}
			default:
				if len(msg.String()) == 1 {
					m.filter += msg.String()
					m.applyFilter()
				}
			}
			return m, m.enrichVisible(c, 24)
		}

		visible := m.visibleIndices()
		switch {
		case msg.String() == "j" || msg.String() == "down":
			if m.cursor < len(visible)-1 {
				m.cursor++
			}
		case msg.String() == "k" || msg.String() == "up":
			if m.cursor > 0 {
				m.cursor--
			}
		case msg.String() == "G":
			m.cursor = max(0, len(visible)-1)
		case msg.String() == "g":
			m.cursor = 0
		case msg.String() == "r":
			m.loading = true
			m.requested = make(map[string]bool)
			m.info = make(map[string]*client.NamespaceInfo)
			return m, m.init(c)
		case msg.String() == "/":
			m.filtering = true
			m.filter = ""
		}
		return m, m.enrichVisible(c, 24)
	}
	return m, nil
}

func (m namespacesModel) view(c *client.Client, width, height int) string {
	if m.loading {
		return loadingStyle.Render("Loading namespaces...")
	}
	if m.err != nil {
		return errorStyle.Render(fmt.Sprintf("Error: %s", m.err)) + "\n\n" +
			helpStyle.Render("q quit")
	}
	if len(m.names) == 0 {
		return "No namespaces found\n\n" + helpStyle.Render("q quit")
	}

	var b strings.Builder
	b.WriteString(headerStyle.Render(fmt.Sprintf(" Namespaces (%d) ", len(m.names))))
	b.WriteString("\n\n")

	nameW := 32
	if width > 100 {
		nameW = width - 46
	}
	rowsW, dimW, idxW := 10, 6, 10
	header := fmt.Sprintf("  %-*s %*s %*s %-*s", nameW, "Name", rowsW, "Rows", dimW, "Dim", idxW, "Indexes")
	b.WriteString(columnHeaderStyle.Render(header))
	b.WriteString("\n")

	visible := m.visibleIndices()
	maxRows := height - 6
	if m.filtering {
		maxRows--
	}
	if maxRows < 1 {
		maxRows = 1
	}
	offset := 0
	if m.cursor >= maxRows {
		offset = m.cursor - maxRows + 1
	}

	for i := offset; i < len(visible) && i < offset+maxRows; i++ {
		name := m.names[visible[i]]
		rows, dim, idx := "…", "…", "…"
		if info, ok := m.info[name]; ok {
			rows = fmt.Sprintf("%d", info.RowCount)
			dim = fmt.Sprintf("%d", info.VectorDim)
			idx = indexFlags(info)
		}
		line := fmt.Sprintf("  %-*s %*s %*s %-*s", nameW, truncate(name, nameW), rowsW, rows, dimW, dim, idxW, idx)
		if i == m.cursor {
			b.WriteString(selectedStyle.Render("▸ " + line[2:]))
		} else {
			b.WriteString(normalStyle.Render(line))
		}
		b.WriteString("\n")
	}

	if m.filtering {
		b.WriteString("\n")
		b.WriteString(statusStyle.Render(fmt.Sprintf("Filter: %s█", m.filter)))
		b.WriteString("\n")
	}

	b.WriteString("\n")
	b.WriteString(statusBar(c))
	b.WriteString("\n")
	b.WriteString(helpStyle.Render("↑/k up • ↓/j down • enter open • i info • / filter • r refresh • q quit"))
	return b.String()
}

// indexFlags renders which indexes are built as a compact V/F/S string.
func indexFlags(info *client.NamespaceInfo) string {
	flag := func(on bool, ch string) string {
		if on {
			return ch
		}
		return "-"
	}
	return flag(info.HasVectorIndex, "V") + flag(info.HasFtsIndex, "F") + flag(info.HasScalarIndex, "S")
}
