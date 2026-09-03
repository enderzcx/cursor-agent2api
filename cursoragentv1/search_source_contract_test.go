package cursoragentv1

import (
	"context"
	"testing"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
)

// Synthetic cases from the S2 source-validity contract. These do not prove
// that a live upstream returned usable references or that client UI rendered.
func TestHostedSearchUnusableReferencesAreNotSuccessfulSources(t *testing.T) {
	for _, source := range []string{"", "  ", "/relative", "https://", "file:///tmp/source", "javascript:alert(1)"} {
		t.Run(source, func(t *testing.T) {
			search := &HostedSearch{CallID: "source-contract", Kind: HostedSearchKindWebSearch, References: []HostedSearchReference{{URL: source, Title: "Unusable source", Chunk: "not a verifiable source"}}}
			kind, payload := hostedSearchResult(search)
			require.Equal(t, "web_search_tool_result", kind)
			errorResult, ok := payload.(map[string]any)
			require.True(t, ok, "nonempty malformed references must not masquerade as successful search results")
			if ok {
				require.Equal(t, "web_search_tool_result_error", errorResult["type"])
				require.Equal(t, "unavailable", errorResult["error_code"])
			}
			require.Empty(t, hostedSearchCitations(search))
		})
	}
}

func TestHostedSearchSourcesUseOneValidityRule(t *testing.T) {
	search := &HostedSearch{CallID: "mixed-source-contract", Kind: HostedSearchKindWebSearch, References: []HostedSearchReference{
		{URL: "", Title: "Missing"},
		{URL: "https://example.com/source", Title: "Real source", Chunk: "source excerpt"},
		{URL: "javascript:alert(1)", Title: "Invalid"},
	}}
	_, payload := hostedSearchResult(search)
	results, ok := payload.([]any)
	require.True(t, ok)
	require.Len(t, results, 1)
	if len(results) == 1 {
		require.Equal(t, "https://example.com/source", results[0].(map[string]any)["url"])
	}
	citations := hostedSearchCitations(search)
	require.Len(t, citations, 1)
	if len(citations) == 1 {
		require.Equal(t, "https://example.com/source", citations[0].(map[string]any)["url"])
	}
}

func TestHostedSearchTrueNoMatchesIsNotAnError(t *testing.T) {
	search := &HostedSearch{CallID: "empty-search-contract", Kind: HostedSearchKindWebSearch}
	kind, payload := hostedSearchResult(search)
	require.Equal(t, "web_search_tool_result", kind)
	results, ok := payload.([]any)
	require.True(t, ok)
	require.Empty(t, results, "zero matches is distinct from malformed nonempty references")
	require.Empty(t, hostedSearchCitations(search))
}

func TestHostedSearchUnusableShapesIncludeBracketWhitespaceAndBareHTTP(t *testing.T) {
	for _, source := range []string{
		"https://[broken",
		"https:// example.com",
		"http://",
		"http://example.com:99999",
		"http://example.com:abc",
		"http://.",
	} {
		t.Run(source, func(t *testing.T) {
			require.False(t, usableHTTPSourceURL(source))
			search := &HostedSearch{CallID: "extra-unusable", Kind: HostedSearchKindWebSearch, References: []HostedSearchReference{{URL: source, Title: "Unusable"}}}
			_, payload := hostedSearchResult(search)
			errorResult, ok := payload.(map[string]any)
			require.True(t, ok, "malformed source must not appear as a successful result list")
			require.Equal(t, "web_search_tool_result_error", errorResult["type"])
			require.Equal(t, "unavailable", errorResult["error_code"])
			require.Empty(t, hostedSearchCitations(search))
			require.Empty(t, hostedSearchActionSources(search))
		})
	}
}

func TestHostedSearchHTTPSourceAndMixedUnusableRefs(t *testing.T) {
	search := &HostedSearch{CallID: "http-mixed", Kind: HostedSearchKindWebSearch, References: []HostedSearchReference{
		{URL: "https://[broken", Title: "Bracket"},
		{URL: "http://example.com/ok", Title: "HTTP source", Chunk: "ok"},
		{URL: "https:// example.com", Title: "Whitespace host"},
	}}
	_, payload := hostedSearchResult(search)
	results, ok := payload.([]any)
	require.True(t, ok)
	require.Len(t, results, 1)
	require.Equal(t, "http://example.com/ok", results[0].(map[string]any)["url"])
	citations := hostedSearchCitations(search)
	require.Len(t, citations, 1)
	require.Equal(t, "http://example.com/ok", citations[0].(map[string]any)["url"])
	sources := hostedSearchActionSources(search)
	require.Len(t, sources, 1)
	require.Equal(t, "http://example.com/ok", sources[0].(map[string]any)["url"])
}

func TestHostedSearchUnusableRefsDoNotFailTheConversation(t *testing.T) {
	projection := newClaudeProjection(context.Background(), translatedTurn{
		publicModel:    "claude-opus-5",
		responseFormat: sdktranslator.FormatClaude,
		managed:        ManagedRequest{SessionID: "msg-unusable-search"},
	})
	search := &HostedSearch{CallID: "ws-bad", Kind: HostedSearchKindWebSearch, Query: "q", References: []HostedSearchReference{{URL: "javascript:alert(1)", Title: "Invalid"}}}
	_, terminal, _, err := projection.handle(Event{Type: EventHostedSearchCompleted, HostedSearch: search})
	require.NoError(t, err)
	require.False(t, terminal)
	_, _, _, err = projection.handle(Event{Type: EventText, Text: "no usable sources"})
	require.NoError(t, err)
	_, terminal, _, err = projection.handle(Event{Type: EventDone})
	require.NoError(t, err)
	require.True(t, terminal)
	payload, err := projection.nonStream()
	require.NoError(t, err)
	output := string(payload)
	require.Contains(t, output, `"type":"web_search_tool_result_error"`)
	require.Contains(t, output, `"error_code":"unavailable"`)
	require.Contains(t, output, `"text":"no usable sources"`)
	require.NotContains(t, output, `"javascript:alert(1)"`)
}
