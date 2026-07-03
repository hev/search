package tui

import "github.com/charmbracelet/lipgloss"

var (
	colorPrimary   = lipgloss.Color("39")  // blue
	colorSecondary = lipgloss.Color("245") // gray
	colorWarning   = lipgloss.Color("214") // orange
	colorMuted     = lipgloss.Color("240") // dark gray

	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("15")).
			Background(colorPrimary).
			Padding(0, 1)

	statusStyle = lipgloss.NewStyle().Foreground(colorSecondary)

	helpStyle = lipgloss.NewStyle().Foreground(colorMuted)

	selectedStyle = lipgloss.NewStyle().Bold(true).Foreground(colorPrimary)

	normalStyle = lipgloss.NewStyle()

	columnHeaderStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(colorSecondary).
				Underline(true)

	errorStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))

	loadingStyle = lipgloss.NewStyle().Foreground(colorWarning)
)
