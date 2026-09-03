package cursoragentv1

import (
	"strings"
	"testing"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
)

func TestCompletedSearchReplayPreservesProtocolProjection(t *testing.T) {
	search := &HostedSearch{CallID: "search-after-tool", Kind: HostedSearchKindWebSearch, Query: "q", References: []HostedSearchReference{{Title: "source", URL: "https://example.com", Chunk: "evidence"}}}
	events := []Event{{Type: EventHostedSearchStarted, HostedSearch: search}, {Type: EventHostedSearchCompleted, HostedSearch: search}, {Type: EventUsage, Tokens: 0}, {Type: EventText, Text: "answer"}, {Type: EventDone}}
	persisted, err := persistEvents(events)
	require.NoError(t, err)
	wire, err := jsonx.Marshal(persisted)
	require.NoError(t, err)
	var restored []persistedEvent
	require.NoError(t, jsonx.Unmarshal(wire, &restored))
	replayed, err := eventsFromPersisted(restored)
	require.NoError(t, err)
	require.Equal(t, events, replayed)
	for _, format := range []sdktranslator.Format{sdktranslator.FormatClaude, sdktranslator.FormatOpenAI, sdktranslator.FormatOpenAIResponse} {
		t.Run(string(format), func(t *testing.T) {
			original, _ := runProjection(t, format, "stable-search-replay", events...)
			replay, _ := runProjection(t, format, "stable-search-replay", replayed...)
			original.respCreatedAt, replay.respCreatedAt = 1, 1
			first, err := original.nonStream()
			require.NoError(t, err)
			second, err := replay.nonStream()
			require.NoError(t, err)
			require.JSONEq(t, string(first), string(second))
		})
	}
}

func TestSearchReplayOwnsSnapshotAndBoundsPayload(t *testing.T) {
	event := Event{Type: EventHostedSearchCompleted, HostedSearch: &HostedSearch{Query: "original", References: []HostedSearchReference{{URL: "https://example.com"}}}}
	cloned := cloneEvent(event)
	persisted, err := persistEvents([]Event{event})
	require.NoError(t, err)
	event.HostedSearch.Query = "mutated"
	event.HostedSearch.References[0].URL = "https://mutated.example"
	require.Equal(t, "original", cloned.HostedSearch.Query)
	require.Equal(t, "https://example.com", cloned.HostedSearch.References[0].URL)
	replayed, err := eventsFromPersisted(persisted)
	require.NoError(t, err)
	require.Equal(t, cloned, replayed[0])
	_, err = persistEvents([]Event{{Type: EventHostedSearchCompleted, HostedSearch: &HostedSearch{Content: strings.Repeat("x", maxCompletedEventBytes+1)}}})
	require.Error(t, err, "hosted search must share the completed-snapshot byte budget")
}
