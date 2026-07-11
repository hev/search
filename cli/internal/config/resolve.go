package config

import "os"

// Endpoint is the resolved connection info for one invocation.
type Endpoint struct {
	// URL is the engine base URL (no trailing slash).
	URL string
	// Profile is the active profile name, or "" if none / env-driven.
	Profile string
}

// Resolve computes the effective endpoint for an invocation.
//
// URL precedence:  --url flag > HEVSEARCH_URL env > active profile > default.
func Resolve(urlFlag string) Endpoint {
	name, profile, ok := GetActiveProfile()

	url := urlFlag
	if url == "" {
		url = os.Getenv("HEVSEARCH_URL")
	}
	if url == "" && ok {
		url = profile.URL
	}
	if url == "" {
		url = DefaultURL
	}
	url = trimTrailingSlash(url)

	pname := ""
	if ok {
		pname = name
	}

	return Endpoint{URL: url, Profile: pname}
}

func trimTrailingSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
