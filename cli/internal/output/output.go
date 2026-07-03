// Package output renders command results in one of three modes:
// human (aligned, styled tables for a TTY), plain (pipe-delimited for
// grep/awk), and json (the machine format for agents). When no mode is
// forced, a TTY gets human and a pipe gets json — this CLI is
// agent-friendly by default.
package output

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"golang.org/x/term"
)

// Mode is an output format.
type Mode string

const (
	ModeHuman Mode = "human"
	ModePlain Mode = "plain"
	ModeJSON  Mode = "json"
)

// CurrentMode is the session's resolved output mode.
var CurrentMode Mode = ModeHuman

// Resolve picks the output mode from an explicit flag, falling back to
// TTY auto-detection: a terminal gets human, a pipe gets json.
func Resolve(explicit string) (Mode, error) {
	switch explicit {
	case "":
		if term.IsTerminal(int(os.Stdout.Fd())) {
			return ModeHuman, nil
		}
		return ModeJSON, nil
	case string(ModeHuman), string(ModePlain), string(ModeJSON):
		return Mode(explicit), nil
	default:
		return "", fmt.Errorf("invalid output mode %q (want human, plain, or json)", explicit)
	}
}

// IsJSON reports whether the session is in json mode.
func IsJSON() bool { return CurrentMode == ModeJSON }

// IsPlain reports whether the session is in plain mode.
func IsPlain() bool { return CurrentMode == ModePlain }

// IsHuman reports whether the session is in human mode.
func IsHuman() bool { return CurrentMode == ModeHuman }

// JSON prints a value as indented JSON to stdout.
func JSON(v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

// JSONRaw prints already-encoded JSON bytes, re-indenting when possible so
// pass-through server responses stay readable.
func JSONRaw(raw []byte) {
	var buf bytes.Buffer
	if json.Indent(&buf, raw, "", "  ") == nil {
		fmt.Println(strings.TrimSpace(buf.String()))
		return
	}
	fmt.Println(strings.TrimSpace(string(raw)))
}

// PrintTablePlain prints a pipe-delimited table.
func PrintTablePlain(headers []string, rows [][]string) {
	fmt.Println(strings.Join(headers, "|"))
	for _, row := range rows {
		fmt.Println(strings.Join(row, "|"))
	}
}

// PrintTable prints a table: pipe-delimited in plain mode, aligned in
// human mode.
func PrintTable(headers []string, rows [][]string) {
	if IsPlain() {
		PrintTablePlain(headers, rows)
		return
	}

	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = lipgloss.Width(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) {
				if w := lipgloss.Width(cell); w > widths[i] {
					widths[i] = w
				}
			}
		}
	}

	var headerLine, sepLine strings.Builder
	for i, h := range headers {
		if i > 0 {
			headerLine.WriteString(" | ")
			sepLine.WriteString("-+-")
		}
		headerLine.WriteString(padVisible(h, widths[i]))
		sepLine.WriteString(strings.Repeat("-", widths[i]))
	}
	fmt.Println(headerLine.String())
	fmt.Println(sepLine.String())

	for _, row := range rows {
		var line strings.Builder
		for i, cell := range row {
			if i > 0 {
				line.WriteString(" | ")
			}
			if i < len(widths) {
				line.WriteString(padVisible(cell, widths[i]))
			} else {
				line.WriteString(cell)
			}
		}
		fmt.Println(line.String())
	}
}

func padVisible(s string, w int) string {
	vw := lipgloss.Width(s)
	if vw >= w {
		return s
	}
	return s + strings.Repeat(" ", w-vw)
}

// Statusf prints a status line to stderr in non-json modes. Kept off
// stdout so it never pollutes machine output.
func Statusf(format string, a ...any) {
	if IsJSON() {
		return
	}
	fmt.Fprintf(os.Stderr, format+"\n", a...)
}

// KeyVal prints an aligned key/value block (human/plain). The key column
// is padded to the widest "key:" so values line up.
func KeyVal(pairs [][2]string) {
	width := 0
	for _, p := range pairs {
		if w := len(p[0]) + 1; w > width {
			width = w
		}
	}
	for _, p := range pairs {
		fmt.Printf("%-*s  %s\n", width, p[0]+":", p[1])
	}
}
