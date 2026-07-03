package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/atotto/clipboard"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
)

// documentModel is the full-screen scrollable JSON view of one document.
type documentModel struct {
	docID    string
	content  string
	viewport viewport.Model
	ready    bool

	toast       string
	toastExpiry time.Time
}

func newDocumentModel(docID, content string) documentModel {
	return documentModel{docID: docID, content: content}
}

func (m *documentModel) setSize(width, height int) {
	w := width - 2
	h := height - 6
	if w < 20 {
		w = 20
	}
	if h < 5 {
		h = 5
	}
	if !m.ready {
		m.viewport = viewport.New(w, h)
		m.viewport.SetContent(m.content)
		m.ready = true
	} else {
		m.viewport.Width = w
		m.viewport.Height = h
	}
}

func (m documentModel) update(msg tea.Msg) documentModel {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.setSize(msg.Width, msg.Height)
		return m
	case tea.KeyMsg:
		if msg.String() == "y" {
			if err := clipboard.WriteAll(m.content); err != nil {
				m.toast = "copy failed: " + err.Error()
			} else {
				m.toast = "Copied document JSON to clipboard"
			}
			m.toastExpiry = time.Now().Add(2 * time.Second)
			return m
		}
	}
	if m.ready {
		var cmd tea.Cmd
		m.viewport, cmd = m.viewport.Update(msg)
		_ = cmd
	}
	return m
}

func (m documentModel) view(width, height int) string {
	if !m.ready {
		m.setSize(width, height)
	}
	var b strings.Builder
	b.WriteString(headerStyle.Render(fmt.Sprintf(" Full JSON: %s ", m.docID)))
	b.WriteString("\n")
	b.WriteString(m.viewport.View())
	b.WriteString("\n")
	if m.toast != "" && time.Now().Before(m.toastExpiry) {
		b.WriteString(statusStyle.Render(m.toast))
		b.WriteString("\n")
	}
	scrollPct := fmt.Sprintf("%3.f%%", m.viewport.ScrollPercent()*100)
	b.WriteString(helpStyle.Render(fmt.Sprintf("↑/k ↓/j • y copy • esc back  %s", scrollPct)))
	return b.String()
}
