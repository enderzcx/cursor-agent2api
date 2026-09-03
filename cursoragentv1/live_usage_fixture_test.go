package cursoragentv1

import (
	"encoding/hex"
	"os"
	"testing"

	agentv1 "github.com/enderzcx/cursor-agent2api/cursoragentv1/gen"
	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// Decoder regression from live numeric bytes, not an assertion that existing
// public output metering is correct. The fixture records a release blocker.
func TestRecordedTerminalUsageAndDeltaDivergence(t *testing.T) {
	data, err := os.ReadFile("testdata/usage-observation/live-20260830.json")
	require.NoError(t, err)
	var fixture struct {
		Samples []struct {
			Case       string  `json:"case"`
			Model      string  `json:"model"`
			Delta      int64   `json:"delta_sum"`
			Hex        *string `json:"terminal_hex"`
			Input      int64   `json:"input"`
			Output     int64   `json:"output"`
			CacheRead  int64   `json:"cache_read"`
			CacheWrite int64   `json:"cache_write"`
			Reasoning  int64   `json:"reasoning"`
		} `json:"samples"`
	}
	require.NoError(t, jsonx.Unmarshal(data, &fixture))
	for _, sample := range fixture.Samples {
		if sample.Hex == nil {
			continue
		}
		t.Run(sample.Model+"/"+sample.Case, func(t *testing.T) {
			wire, err := hex.DecodeString(*sample.Hex)
			require.NoError(t, err)
			var terminal agentv1.TurnEndedUpdate
			require.NoError(t, proto.Unmarshal(wire, &terminal))
			obs := observeTurnEndedUpdate(&terminal)
			require.Equal(t, ObservedCount{Present: true, Value: sample.Input}, obs.Input)
			require.Equal(t, ObservedCount{Present: true, Value: sample.Output}, obs.Output)
			require.Equal(t, ObservedCount{Present: true, Value: sample.CacheRead}, obs.CacheRead)
			require.Equal(t, ObservedCount{Present: true, Value: sample.CacheWrite}, obs.CacheWrite)
			require.Equal(t, ObservedCount{Present: true, Value: sample.Reasoning}, obs.Reasoning)
			require.NotEqual(t, sample.Delta, obs.Output.Value, "this captured counterexample disproves exact delta-to-output equivalence")
		})
	}
}

// Protect the independently observed scope and arithmetic, without enabling a
// billing policy or treating first-poll Dashboard data as immutable receipts.
func TestRecordedTurnScopeDashboardConvergence(t *testing.T) {
	data, err := os.ReadFile("testdata/usage-observation/turn-scope-20260830.json")
	require.NoError(t, err)
	var fixture struct {
		Calls      int `json:"calls"`
		Streams    int `json:"streams"`
		ATerminals int `json:"a_terminal_count"`
		Turns      []struct {
			Input           int64 `json:"wire_input"`
			Output          int64 `json:"wire_output"`
			Read            int64 `json:"wire_cache_read"`
			Write           int64 `json:"wire_cache_write"`
			Timestamp       int64 `json:"dashboard_timestamp"`
			DashboardInput  int64 `json:"dashboard_input"`
			DashboardOutput int64 `json:"dashboard_output"`
			DashboardRead   int64 `json:"dashboard_cache_read"`
			Delta           int64 `json:"delta_sum"`
		} `json:"turns"`
		Transient struct {
			Timestamp int64 `json:"timestamp"`
			Output    int64 `json:"output"`
		} `json:"transient_dashboard"`
	}
	require.NoError(t, jsonx.Unmarshal(data, &fixture))
	require.Equal(t, 3, fixture.Calls)
	require.Equal(t, 2, fixture.Streams)
	require.Zero(t, fixture.ATerminals)
	require.Len(t, fixture.Turns, 2)
	for _, turn := range fixture.Turns {
		require.Equal(t, turn.Input-turn.Read-turn.Write, turn.DashboardInput)
		require.Equal(t, turn.Output, turn.DashboardOutput)
		require.Equal(t, turn.Read, turn.DashboardRead)
		require.NotEqual(t, turn.Delta, turn.Output)
	}
	require.Less(t, fixture.Turns[1].Output, fixture.Turns[0].Output, "later turn is not lifetime-cumulative output")
	require.NotEqual(t, fixture.Turns[0].Timestamp, fixture.Turns[1].Timestamp)
	require.Equal(t, fixture.Turns[0].Timestamp, fixture.Transient.Timestamp)
	require.Less(t, fixture.Transient.Output, fixture.Turns[0].DashboardOutput, "one dashboard row is asynchronously updated")
}
