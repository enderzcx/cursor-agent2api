package cursoragentv1

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// Exercise startup through NewFileStateStore, not a private rebuild helper.
// Reconstructable index contents do not make deleting a live lock inode safe:
// another store must coordinate with the same inode, or fail/retry its startup.
func TestAdversarialStoreStartupPreservesLiveToolIndexLock(t *testing.T) {
	for _, indexDir := range []string{"pending-index", "completed-index"} {
		t.Run(indexDir, func(t *testing.T) {
			root := t.TempDir()
			store, err := NewFileStateStore(root)
			require.NoError(t, err)
			request := managedTestRequest("adversarial-lock")
			fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
			require.NoError(t, err)
			claim, err := store.Claim(request.SessionID, "conversation-lock", fingerprint, ClaimOptions{
				ToolCatalogDigest: request.ToolCatalogDigest, ToolCatalog: request.Run.Tools,
			})
			require.NoError(t, err)
			if indexDir == "pending-index" {
				require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-lock", Name: "lookup"}}))
			} else {
				require.NoError(t, store.SaveCompleted(claim.Owner, []ToolResult{{ToolCallID: "tool-lock", Content: "ok"}}, []Event{{Type: EventDone}}, false, ""))
			}
			require.NoError(t, store.Release(claim.Owner))

			toolDir := filepath.Join(root, indexDir, "by-tool")
			entries, err := os.ReadDir(toolDir)
			if os.IsNotExist(err) {
				t.Skip("base predates by-tool indexes; run this startup regression against the index implementation")
			}
			require.NoError(t, err)
			lockPath := ""
			for _, entry := range entries {
				if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".lock") {
					lockPath = filepath.Join(toolDir, entry.Name())
					break
				}
			}
			require.NotEmpty(t, lockPath, "writing one indexed tool must publish its coordination lock")
			lockFile, err := os.OpenFile(lockPath, os.O_RDWR, 0)
			require.NoError(t, err)
			defer lockFile.Close()
			require.NoError(t, unix.Flock(int(lockFile.Fd()), unix.LOCK_EX|unix.LOCK_NB))
			defer unix.Flock(int(lockFile.Fd()), unix.LOCK_UN) //nolint:errcheck
			before, err := lockFile.Stat()
			require.NoError(t, err)

			startup := make(chan error, 1)
			go func() {
				_, openErr := NewFileStateStore(root)
				startup <- openErr
			}()
			// Either nonblocking refusal or waiting for the owner is safe. Neither
			// may remove/recreate the coordination inode while it is still owned.
			finished := false
			select {
			case <-startup:
				finished = true
			case <-time.After(2 * time.Second):
			}
			after, statErr := os.Stat(lockPath)
			assert.NoError(t, statErr, "startup must not unlink another store's live lock")
			if statErr == nil {
				assert.True(t, os.SameFile(before, after), "startup replaced the held lock inode and bypassed cross-store exclusion")
			}
			require.NoError(t, unix.Flock(int(lockFile.Fd()), unix.LOCK_UN))
			if !finished {
				select {
				case <-startup:
				case <-time.After(3 * time.Second):
					t.Fatal("startup did not finish after the held index lock was released")
				}
			}
		})
	}
}
