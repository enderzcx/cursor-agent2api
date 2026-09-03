package plugin

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/enderzcx/cursor-agent2api/cursoragentv1"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"github.com/stretchr/testify/require"
)

func TestParseAuthHandlesCursorAgentV1File(t *testing.T) {
	p := testPlugin(t)
	resp, err := p.ParseAuth(context.Background(), pluginapi.AuthParseRequest{
		FileName: "cursor.json",
		RawJSON:  []byte(`{"type":"cursor-agent-v1","api_key":"key_test","account_id":"acct-1","email":"user@example.com"}`),
	})
	require.NoError(t, err)
	require.True(t, resp.Handled)
	require.Equal(t, cursoragentv1.ProviderID, resp.Auth.Provider)
	require.Equal(t, "user@example.com", resp.Auth.Label)
	require.Equal(t, "key_test", resp.Auth.Metadata["api_key"])
	require.Equal(t, "acct-1", resp.Auth.Metadata["account_id"])
}

func TestParseAuthIgnoresOtherProviders(t *testing.T) {
	p := testPlugin(t)
	resp, err := p.ParseAuth(context.Background(), pluginapi.AuthParseRequest{
		FileName: "openai.json",
		RawJSON:  []byte(`{"type":"openai","api_key":"sk-test"}`),
	})
	require.NoError(t, err)
	require.False(t, resp.Handled)
}

func TestParseAuthAcceptsUntypedCursorAPIKey(t *testing.T) {
	p := testPlugin(t)
	resp, err := p.ParseAuth(context.Background(), pluginapi.AuthParseRequest{
		FileName: "key.json",
		RawJSON:  []byte(`{"api_key":"key_abc123"}`),
	})
	require.NoError(t, err)
	require.True(t, resp.Handled)
	require.Equal(t, cursoragentv1.ProviderID, resp.Auth.Provider)
}

func TestBuildUsesConfiguredStateDir(t *testing.T) {
	dir := t.TempDir()
	p, err := Build([]byte("state_dir: " + dir + "\n"))
	require.NoError(t, err)
	require.Equal(t, cursoragentv1.ProviderID, p.Identifier())
	abs, err := filepath.Abs(dir)
	require.NoError(t, err)
	require.Equal(t, abs, p.stateDir)
}

func TestStaticModelsMatchDefaultCatalog(t *testing.T) {
	p := testPlugin(t)
	resp, err := p.StaticModels(context.Background(), pluginapi.StaticModelRequest{})
	require.NoError(t, err)
	require.Equal(t, cursoragentv1.ProviderID, resp.Provider)
	require.Len(t, resp.Models, len(cursoragentv1.DefaultModels))
	require.Equal(t, cursoragentv1.DefaultModels[0], resp.Models[0].ID)
}

func testPlugin(t *testing.T) *Plugin {
	t.Helper()
	p, err := Build([]byte("state_dir: " + t.TempDir() + "\n"))
	require.NoError(t, err)
	return p
}
