package cursoragentv1

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResumeFlightFirstResultIsImmutable(t *testing.T) {
	manager := &SessionManager{flights: make(map[string]*resumeFlight)}
	fingerprint := "fp"
	batch := []string{"tool-1"}
	flight, leader, err := manager.beginResumeFlight(fingerprint, batch, "digest", "")
	require.NoError(t, err)
	require.True(t, leader)

	first := CompletedReplay{SessionID: "session-keep", ConversationID: "conv-keep", BillingRunID: "bill-keep"}
	manager.endResumeFlight(fingerprint, batch, flight, &first, nil)
	manager.endResumeFlight(fingerprint, batch, flight, nil, errors.New("late finisher"))

	output, waitErr := waitResumeFlight(context.Background(), flight)
	require.NoError(t, waitErr)
	require.Equal(t, "session-keep", output.SessionID)
	require.Equal(t, "bill-keep", output.BillingRunID)
	require.Nil(t, flight.err)
	require.Equal(t, "session-keep", flight.replay.SessionID)
}

func TestResumeFlightConcurrentFinishAndWait(t *testing.T) {
	manager := &SessionManager{flights: make(map[string]*resumeFlight)}
	fingerprint := "fp"
	batch := []string{"tool-1"}
	flight, leader, err := manager.beginResumeFlight(fingerprint, batch, "digest", "")
	require.NoError(t, err)
	require.True(t, leader)

	first := CompletedReplay{SessionID: "session-keep", BillingRunID: "bill-keep"}
	manager.endResumeFlight(fingerprint, batch, flight, &first, nil)

	var wg sync.WaitGroup
	const n = 16
	outputs := make([]*ManagedOutput, n)
	waitErrs := make([]error, n)
	wg.Add(n * 2)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			manager.endResumeFlight(fingerprint, batch, flight, nil, errors.New("late finisher"))
		}()
		go func(i int) {
			defer wg.Done()
			outputs[i], waitErrs[i] = waitResumeFlight(context.Background(), flight)
		}(i)
	}
	wg.Wait()

	require.Nil(t, flight.err)
	require.Equal(t, "session-keep", flight.replay.SessionID)
	for i := 0; i < n; i++ {
		require.NoError(t, waitErrs[i])
		require.NotNil(t, outputs[i])
		require.Equal(t, "session-keep", outputs[i].SessionID)
		require.Equal(t, "bill-keep", outputs[i].BillingRunID)
	}
}

func TestResumeFlightLateFinisherDoesNotReplaceNewFlight(t *testing.T) {
	manager := &SessionManager{flights: make(map[string]*resumeFlight)}
	fingerprint := "fp"
	batch := []string{"tool-1"}
	firstFlight, leader, err := manager.beginResumeFlight(fingerprint, batch, "digest-1", "")
	require.NoError(t, err)
	require.True(t, leader)
	first := CompletedReplay{SessionID: "session-old"}
	manager.endResumeFlight(fingerprint, batch, firstFlight, &first, nil)

	secondFlight, leader, err := manager.beginResumeFlight(fingerprint, batch, "digest-2", "")
	require.NoError(t, err)
	require.True(t, leader)
	require.NotSame(t, firstFlight, secondFlight)
	key := resumeFlightKey(fingerprint, batch, "")
	manager.mu.Lock()
	require.Equal(t, secondFlight, manager.flights[key])
	manager.mu.Unlock()

	manager.endResumeFlight(fingerprint, batch, firstFlight, nil, errors.New("stale owner"))

	manager.mu.Lock()
	require.Equal(t, secondFlight, manager.flights[key])
	manager.mu.Unlock()
	select {
	case <-secondFlight.done:
		t.Fatal("replacement flight must stay unpublished")
	default:
	}
	require.Nil(t, secondFlight.err)
	require.Nil(t, secondFlight.replay)
	require.Nil(t, firstFlight.err)
	require.Equal(t, "session-old", firstFlight.replay.SessionID)
}
