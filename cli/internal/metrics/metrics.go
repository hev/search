// Package metrics parses the engine's Prometheus text exposition into a
// small structured form the CLI can filter and render.
package metrics

import (
	"strconv"
	"strings"
)

// Sample is one metric time series line.
type Sample struct {
	Name   string  `json:"name"`
	Labels string  `json:"labels,omitempty"`
	Value  float64 `json:"value"`
}

// CuratedSubstrings are the metric-name fragments shown by default: the
// operator-relevant cache, storage, and query counters.
var CuratedSubstrings = []string{"cache", "s3_request", "query", "index_build", "compaction"}

// Parse extracts samples from Prometheus text, skipping HELP/TYPE comments.
func Parse(text string) []Sample {
	var out []Sample
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, labels, value, ok := parseLine(line)
		if !ok {
			continue
		}
		out = append(out, Sample{Name: name, Labels: labels, Value: value})
	}
	return out
}

func parseLine(line string) (name, labels string, value float64, ok bool) {
	// Split off the trailing value (last whitespace-separated field).
	sp := strings.LastIndexAny(line, " \t")
	if sp < 0 {
		return "", "", 0, false
	}
	metric := strings.TrimSpace(line[:sp])
	valStr := strings.TrimSpace(line[sp+1:])
	v, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return "", "", 0, false
	}
	name = metric
	if br := strings.IndexByte(metric, '{'); br >= 0 {
		name = metric[:br]
		labels = strings.TrimSuffix(metric[br:], "")
	}
	return name, labels, v, true
}

// Filter returns samples whose name contains any of the given substrings.
// An empty substrings slice returns everything.
func Filter(samples []Sample, substrings []string) []Sample {
	if len(substrings) == 0 {
		return samples
	}
	var out []Sample
	for _, s := range samples {
		for _, sub := range substrings {
			if strings.Contains(s.Name, sub) {
				out = append(out, s)
				break
			}
		}
	}
	return out
}

// FilterPrefix returns samples whose name has the given prefix.
func FilterPrefix(samples []Sample, prefix string) []Sample {
	if prefix == "" {
		return samples
	}
	var out []Sample
	for _, s := range samples {
		if strings.HasPrefix(s.Name, prefix) {
			out = append(out, s)
		}
	}
	return out
}
