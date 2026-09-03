package cursoragentv1

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockedReleaseStore struct {
	StateStore
	entered chan struct{}
	unblock chan struct{}
}

func (s *blockedReleaseStore) Release(owner OwnerLease) error {
	close(s.entered)
	<-s.unblock
	return s.StateStore.Release(owner)
}

func TestSessionRemainsActiveUntilStateReleaseCompletes(t *testing.T) {
	base, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	store := &blockedReleaseStore{StateStore: base, entered: make(chan struct{}), unblock: make(chan struct{})}
	manager, err := newSessionManager(&scriptedEngine{run: func(_ context.Context, _ RunRequest, emit func(Event)) error {
		emit(Event{Type: EventDone})
		return nil
	}}, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("finish-boundary")
	output, err := manager.Start(request)
	require.NoError(t, err)
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		close(store.unblock)
		t.Fatal("session did not reach state release")
	}
	manager.mu.Lock()
	_, active := manager.active[request.SessionID]
	manager.mu.Unlock()
	close(store.unblock)
	for range output.Events {
	}
	require.NoError(t, <-output.Result)
	require.True(t, active, "idle must not be published while state finalization still owns the lease")
}
