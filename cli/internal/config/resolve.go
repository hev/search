package config

import "os"

// Endpoint is the resolved connection info for one invocation.
type Endpoint struct {
	// URL is the engine base URL (no trailing slash).
	URL string
	// APIKey is the read-path bearer token, if any.
	APIKey string
	// AdminAPIKey is the admin-path bearer token, if any. Falls back to
	// APIKey when unset — the engine is a single trusted service.
	AdminAPIKey string
	// Profile is the active profile name, or "" if none / env-driven.
	Profile string
}

// Resolve computes the effective endpoint for an invocation.
//
// URL precedence:  --url flag > HEVSEARCH_URL env > active profile > default.
// Key precedence:  HEVSEARCH_API_KEY / HEVSEARCH_ADMIN_API_KEY env >
//
//	active profile api_key / admin_api_key.
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

	apiKey := os.Getenv("HEVSEARCH_API_KEY")
	if apiKey == "" && ok {
		apiKey = profile.APIKey
	}

	adminKey := os.Getenv("HEVSEARCH_ADMIN_API_KEY")
	if adminKey == "" && ok {
		adminKey = profile.AdminAPIKey
	}
	if adminKey == "" {
		adminKey = apiKey
	}

	pname := ""
	if ok {
		pname = name
	}

	return Endpoint{URL: url, APIKey: apiKey, AdminAPIKey: adminKey, Profile: pname}
}

func trimTrailingSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
