// Package config manages hev CLI profiles stored in ~/.hevsearch/config.toml.
//
// A profile names an engine endpoint (base URL) plus the TUI preview content
// field. hev search is a trusted internal service; Layer owns auth at the edge.
package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// DefaultURL is the engine's default internal REST endpoint.
const DefaultURL = "http://localhost:3000"

var configDir = filepath.Join(homeDir(), ".hevsearch")
var configFile = filepath.Join(configDir, "config.toml")

func homeDir() string {
	h, _ := os.UserHomeDir()
	return h
}

// Config is the on-disk config: an active profile name and a map of
// named profiles.
type Config struct {
	Active   string                   `toml:"active"`
	Profiles map[string]ProfileConfig `toml:"profiles"`
}

// ProfileConfig is a single named endpoint.
type ProfileConfig struct {
	URL          string `toml:"url"`
	ContentField string `toml:"content_field,omitempty"`
	// ContentFields holds per-namespace preview overrides for the TUI.
	ContentFields map[string]string `toml:"content_fields,omitempty"`
}

// GetContentField returns the preview content field for a namespace,
// preferring a namespace-specific override over the profile default.
func (p ProfileConfig) GetContentField(namespace string) string {
	if p.ContentFields != nil {
		if f, ok := p.ContentFields[namespace]; ok {
			return f
		}
	}
	return p.ContentField
}

// Load reads config from ~/.hevsearch/config.toml. Returns an empty
// config if the file does not exist or cannot be parsed.
func Load() Config {
	cfg := Config{Profiles: make(map[string]ProfileConfig)}
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		return cfg
	}
	if _, err := toml.DecodeFile(configFile, &cfg); err != nil {
		return Config{Profiles: make(map[string]ProfileConfig)}
	}
	if cfg.Profiles == nil {
		cfg.Profiles = make(map[string]ProfileConfig)
	}
	return cfg
}

// Save writes config to ~/.hevsearch/config.toml with 0600 perms.
func Save(cfg Config) error {
	if err := os.MkdirAll(configDir, 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(configFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return toml.NewEncoder(f).Encode(cfg)
}

// GetActiveProfile returns the active profile's name and config.
func GetActiveProfile() (string, ProfileConfig, bool) {
	cfg := Load()
	if cfg.Active == "" {
		return "", ProfileConfig{}, false
	}
	p, ok := cfg.Profiles[cfg.Active]
	if !ok {
		return "", ProfileConfig{}, false
	}
	return cfg.Active, p, true
}

// ProfileEntry pairs a profile with its name and active status.
type ProfileEntry struct {
	Name     string
	Config   ProfileConfig
	IsActive bool
}

// ListProfiles returns all configured profiles.
func ListProfiles() []ProfileEntry {
	cfg := Load()
	var entries []ProfileEntry
	for name, p := range cfg.Profiles {
		entries = append(entries, ProfileEntry{
			Name:     name,
			Config:   p,
			IsActive: name == cfg.Active,
		})
	}
	return entries
}

// AddProfile adds or overwrites a profile, making it active if it is the
// first one configured.
func AddProfile(name, url string) error {
	cfg := Load()
	cfg.Profiles[name] = ProfileConfig{
		URL: url,
	}
	if cfg.Active == "" {
		cfg.Active = name
	}
	return Save(cfg)
}

// RemoveProfile deletes a profile, reassigning the active pointer if the
// removed profile was active.
func RemoveProfile(name string) error {
	cfg := Load()
	if _, ok := cfg.Profiles[name]; !ok {
		return fmt.Errorf("profile %q not found", name)
	}
	delete(cfg.Profiles, name)
	if cfg.Active == name {
		cfg.Active = ""
		for k := range cfg.Profiles {
			cfg.Active = k
			break
		}
	}
	return Save(cfg)
}

// SetActive marks a profile as active.
func SetActive(name string) error {
	cfg := Load()
	if _, ok := cfg.Profiles[name]; !ok {
		return fmt.Errorf("profile %q not found", name)
	}
	cfg.Active = name
	return Save(cfg)
}

// GetActiveContentField returns the preview content field for the active
// profile and namespace.
func GetActiveContentField(namespace string) string {
	_, p, ok := GetActiveProfile()
	if !ok {
		return ""
	}
	return p.GetContentField(namespace)
}

// SetContentField sets the preview content field for the active profile.
// An empty namespace sets the profile default; a non-empty one sets a
// per-namespace override. An empty field clears the setting.
func SetContentField(field, namespace string) error {
	cfg := Load()
	if cfg.Active == "" {
		return fmt.Errorf("no active profile")
	}
	p := cfg.Profiles[cfg.Active]
	if namespace == "" {
		p.ContentField = field
	} else {
		if p.ContentFields == nil {
			p.ContentFields = make(map[string]string)
		}
		if field == "" {
			delete(p.ContentFields, namespace)
		} else {
			p.ContentFields[namespace] = field
		}
	}
	cfg.Profiles[cfg.Active] = p
	return Save(cfg)
}

// MaskKey masks an API key, showing only its first 8 characters.
func MaskKey(key string) string {
	if key == "" {
		return "(none)"
	}
	if len(key) <= 8 {
		return "********"
	}
	return key[:8] + "..."
}
