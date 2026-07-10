package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// redirectConfig points the package at a temp config file for the test.
func redirectConfig(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	oldDir, oldFile := configDir, configFile
	configDir = dir
	configFile = filepath.Join(dir, "config.toml")
	t.Cleanup(func() { configDir, configFile = oldDir, oldFile })
}

func TestSaveLoadRoundTrip(t *testing.T) {
	redirectConfig(t)

	require.NoError(t, AddProfile("local", "http://localhost:3000"))
	require.NoError(t, AddProfile("staging", "https://staging:3000"))
	require.NoError(t, SetActive("staging"))

	name, p, ok := GetActiveProfile()
	require.True(t, ok)
	assert.Equal(t, "staging", name)
	assert.Equal(t, "https://staging:3000", p.URL)

	_, ok = Load().Profiles["local"]
	require.True(t, ok)
}

func TestSaveUses0600(t *testing.T) {
	redirectConfig(t)
	require.NoError(t, AddProfile("local", "http://localhost:3000"))
	info, err := os.Stat(configFile)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm())
}

func TestRemoveProfileReassignsActive(t *testing.T) {
	redirectConfig(t)
	require.NoError(t, AddProfile("a", "http://a"))
	require.NoError(t, AddProfile("b", "http://b"))
	require.NoError(t, SetActive("a"))
	require.NoError(t, RemoveProfile("a"))

	name, _, ok := GetActiveProfile()
	require.True(t, ok)
	assert.Equal(t, "b", name, "active should fall through to a remaining profile")
}

func TestContentFieldOverride(t *testing.T) {
	redirectConfig(t)
	require.NoError(t, AddProfile("local", "http://localhost:3000"))
	require.NoError(t, SetContentField("body", ""))
	require.NoError(t, SetContentField("title", "books"))

	assert.Equal(t, "body", GetActiveContentField("other"))
	assert.Equal(t, "title", GetActiveContentField("books"))
}

func TestResolvePrecedence(t *testing.T) {
	redirectConfig(t)
	require.NoError(t, AddProfile("local", "http://profile-url:3000"))
	require.NoError(t, SetActive("local"))

	t.Run("flag beats env and profile", func(t *testing.T) {
		t.Setenv("HEVSEARCH_URL", "http://env-url:3000")
		ep := Resolve("http://flag-url:3000")
		assert.Equal(t, "http://flag-url:3000", ep.URL)
	})

	t.Run("env beats profile", func(t *testing.T) {
		t.Setenv("HEVSEARCH_URL", "http://env-url:3000")
		ep := Resolve("")
		assert.Equal(t, "http://env-url:3000", ep.URL)
	})

	t.Run("profile beats default", func(t *testing.T) {
		os.Unsetenv("HEVSEARCH_URL")
		ep := Resolve("")
		assert.Equal(t, "http://profile-url:3000", ep.URL)
		assert.Equal(t, "local", ep.Profile)
	})
}

func TestResolveTrimsTrailingSlash(t *testing.T) {
	redirectConfig(t)
	ep := Resolve("http://localhost:3000/")
	assert.Equal(t, "http://localhost:3000", ep.URL)
}

func TestResolveDefaultWhenEmpty(t *testing.T) {
	redirectConfig(t)
	os.Unsetenv("HEVSEARCH_URL")
	ep := Resolve("")
	assert.Equal(t, DefaultURL, ep.URL)
}
