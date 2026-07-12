package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/hev/search/cli/internal/client"
)

type infoLoadedMsg struct{ info client.NamespaceInfo }
type infoErrMsg struct{ err error }

// infoModel is the namespace info / schema panel.
type infoModel struct {
	namespace string
	info      *client.NamespaceInfo
	loading   bool
	err       error
}

func newInfoModel(namespace string) infoModel {
	return infoModel{namespace: namespace, loading: true}
}

func (m infoModel) init(c *client.Client) tea.Cmd {
	ns := m.namespace
	return func() tea.Msg {
		info, _, err := c.Info(ctxbg(), ns)
		if err != nil {
			return infoErrMsg{err: err}
		}
		return infoLoadedMsg{info: info}
	}
}

func (m infoModel) update(msg tea.Msg) (infoModel, tea.Cmd) {
	switch msg := msg.(type) {
	case infoLoadedMsg:
		m.info = &msg.info
		m.loading = false
	case infoErrMsg:
		m.err = msg.err
		m.loading = false
	}
	return m, nil
}

func (m infoModel) view(width, height int) string {
	var b strings.Builder
	b.WriteString(headerStyle.Render(fmt.Sprintf(" Namespace: %s ", m.namespace)))
	b.WriteString("\n\n")

	if m.loading {
		b.WriteString(loadingStyle.Render("Loading namespace info..."))
		b.WriteString("\n\n")
		b.WriteString(helpStyle.Render("esc back"))
		return b.String()
	}
	if m.err != nil {
		b.WriteString(errorStyle.Render(fmt.Sprintf("Error: %s", m.err)))
		b.WriteString("\n\n")
		b.WriteString(helpStyle.Render("esc back"))
		return b.String()
	}

	info := m.info
	lastWrite := "unavailable"
	if info.LastWriteMs != nil {
		lastWrite = fmt.Sprintf("%d", *info.LastWriteMs)
	}
	logicalBytes := "unavailable"
	if info.ApproxLogicalBytes != nil {
		logicalBytes = fmt.Sprintf("%d", *info.ApproxLogicalBytes)
	}
	fields := [][2]string{
		{"kind", info.Kind},
		{"vector_dim", fmt.Sprintf("%d", info.VectorDim)},
		{"id_type", info.IDType},
		{"distance_metric", info.DistanceMetric},
		{"row_count", fmt.Sprintf("%d", info.RowCount)},
		{"fragment_count", fmt.Sprintf("%d", info.FragmentCount)},
		{"has_vector_index", fmt.Sprintf("%t", info.HasVectorIndex)},
		{"has_fts_index", fmt.Sprintf("%t", info.HasFtsIndex)},
		{"has_scalar_index", fmt.Sprintf("%t", info.HasScalarIndex)},
		{"table_version", fmt.Sprintf("%d", info.TableVersion)},
		{"last_write_ms", lastWrite},
		{"approx_logical_bytes", logicalBytes},
		{"schema_fields", fmt.Sprintf("%d", len(info.Schema))},
	}
	width = 0
	for _, f := range fields {
		if len(f[0]) > width {
			width = len(f[0])
		}
	}
	for _, f := range fields {
		b.WriteString(fmt.Sprintf("  %-*s  %s\n", width, f[0], f[1]))
	}
	b.WriteString("\n")
	b.WriteString(helpStyle.Render("esc back • i close"))
	return b.String()
}
