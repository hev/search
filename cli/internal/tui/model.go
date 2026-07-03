// Package tui is the Bubble Tea view-stack browser for the hev search
// engine: namespaces → documents → preview → full JSON, with a namespace
// info panel. It reads the same REST API the commands use.
package tui

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/hev/search/cli/internal/client"
	"github.com/hev/search/cli/internal/config"
)

type view int

const (
	viewNamespaces view = iota
	viewDocuments
	viewPreview
	viewDocument
	viewInfo
)

// Model is the top-level Bubble Tea model.
type Model struct {
	client *client.Client
	view   view
	width  int
	height int

	namespaces namespacesModel
	documents  documentsModel
	preview    previewModel
	document   documentModel
	info       infoModel

	selectedNamespace string
	prevView          view
}

// New creates a TUI model bound to a client.
func New(c *client.Client) Model {
	return Model{
		client:     c,
		view:       viewNamespaces,
		namespaces: newNamespacesModel(),
	}
}

func (m Model) Init() tea.Cmd {
	return m.namespaces.init(m.client)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			return m, tea.Quit
		}
	}

	switch m.view {
	case viewNamespaces:
		return m.updateNamespaces(msg)
	case viewDocuments:
		return m.updateDocuments(msg)
	case viewPreview:
		return m.updatePreview(msg)
	case viewDocument:
		return m.updateDocument(msg)
	case viewInfo:
		return m.updateInfo(msg)
	}
	return m, nil
}

func (m Model) View() string {
	switch m.view {
	case viewNamespaces:
		return m.namespaces.view(m.client, m.width, m.height)
	case viewDocuments:
		return m.documents.view(m.width, m.height)
	case viewPreview:
		return m.preview.view(m.width, m.height)
	case viewDocument:
		return m.document.view(m.width, m.height)
	case viewInfo:
		return m.info.view(m.width, m.height)
	}
	return ""
}

func (m Model) updateNamespaces(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch {
		case msg.String() == "q" && !m.namespaces.filtering:
			return m, tea.Quit
		case msg.String() == "enter" && !m.namespaces.filtering:
			if ns := m.namespaces.selected(); ns != "" {
				m.selectedNamespace = ns
				m.documents = newDocumentsModel(ns, config.GetActiveContentField(ns))
				m.view = viewDocuments
				return m, m.documents.init(m.client)
			}
		case msg.String() == "i" && !m.namespaces.filtering:
			if ns := m.namespaces.selected(); ns != "" {
				m.selectedNamespace = ns
				m.prevView = viewNamespaces
				m.info = newInfoModel(ns)
				m.view = viewInfo
				return m, m.info.init(m.client)
			}
		}
	}
	var cmd tea.Cmd
	m.namespaces, cmd = m.namespaces.update(msg, m.client)
	return m, cmd
}

func (m Model) updateDocuments(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if m.documents.searching {
			break
		}
		switch {
		case msg.String() == "esc":
			if m.documents.searchActive {
				break
			}
			m.view = viewNamespaces
			return m, nil
		case msg.String() == "q":
			m.view = viewNamespaces
			return m, nil
		case msg.String() == "i":
			m.prevView = viewDocuments
			m.info = newInfoModel(m.selectedNamespace)
			m.view = viewInfo
			return m, m.info.init(m.client)
		case msg.String() == "enter":
			if content, id, ok := m.documents.selectedJSON(); ok {
				m.preview = newPreviewModel(id, content)
				m.preview.setSize(m.width, m.height)
				m.view = viewPreview
				return m, nil
			}
		}
	}
	var cmd tea.Cmd
	m.documents, cmd = m.documents.update(msg, m.client)
	return m, cmd
}

func (m Model) updatePreview(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch {
		case msg.String() == "esc" || msg.String() == "q":
			m.view = viewDocuments
			return m, nil
		case msg.String() == "enter":
			m.document = newDocumentModel(m.preview.docID, m.preview.content)
			m.document.setSize(m.width, m.height)
			m.view = viewDocument
			return m, nil
		}
	}
	m.preview = m.preview.update(msg)
	return m, nil
}

func (m Model) updateDocument(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "esc" || msg.String() == "q" {
			m.view = viewPreview
			return m, nil
		}
	}
	m.document = m.document.update(msg)
	return m, nil
}

func (m Model) updateInfo(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "esc" || msg.String() == "q" || msg.String() == "i" {
			m.view = m.prevView
			return m, nil
		}
	}
	var cmd tea.Cmd
	m.info, cmd = m.info.update(msg)
	return m, cmd
}
