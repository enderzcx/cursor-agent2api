package cursoragentv1

import (
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
)

// Reasoning replay policy for the Sand data plane, fixed by live A/B
// (TestAuthorizedSandReasoningReplayProbe, TestAuthorizedSandForeignSignatureCostProbe):
//
//	A  Cursor-minted signature replayed to Cursor        -> accepted (also across Cursor models)
//	B  foreign (Anthropic/Kiro/fake) signature replayed  -> resource_exhausted before any output
//	C  reasoning text without signature                  -> accepted
//
// Claude Code resends the full history every turn, so a session migrated from
// another provider would otherwise pay B's failed round trip (4-19 s measured)
// on every message. The sidecar therefore keeps a bounded in-memory memory of
// signatures Cursor minted through it and forwards only those; any other
// signature is sent text-only (case C) with no upstream round trip. Should a
// remembered signature still be rejected, the runner retries once text-only and
// marks it rejected. redacted_thinking ciphertext is never forwarded.
var (
	sandReasoningSignatureForwarding = true
	sandRedactedReasoningForwarding  = false
	// sandSignatureRetries counts text-only replays taken after a signature
	// rejection; exposed for probes and operational readback.
	sandSignatureRetries atomic.Int64
	signatureMemory      = newSignatureMemory(8192)
)

func SandSignatureRetryCount() int64 { return sandSignatureRetries.Load() }

type signatureState uint8

const (
	signatureUnknown signatureState = iota
	signatureMinted
	signatureRejected
)

// signatureMemoryStore is a bounded FIFO map keyed by sha256(signature).
type signatureMemoryStore struct {
	mu    sync.Mutex
	limit int
	order []string
	state map[string]signatureState
}

func newSignatureMemory(limit int) *signatureMemoryStore {
	return &signatureMemoryStore{limit: limit, state: make(map[string]signatureState, limit)}
}

func signatureKey(signature string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(signature)))
	return string(digest[:])
}

func (m *signatureMemoryStore) lookup(signature string) signatureState {
	if m == nil || strings.TrimSpace(signature) == "" {
		return signatureUnknown
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state[signatureKey(signature)]
}

func (m *signatureMemoryStore) record(signature string, state signatureState) {
	if m == nil || strings.TrimSpace(signature) == "" {
		return
	}
	key := signatureKey(signature)
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.state[key]; !exists {
		m.order = append(m.order, key)
		for len(m.order) > m.limit {
			delete(m.state, m.order[0])
			m.order = m.order[1:]
		}
	}
	m.state[key] = state
}

// rememberMintedSignature is called when Sand returns a thinking signature so
// the same session can replay it natively on later turns.
func rememberMintedSignature(signature string) { signatureMemory.record(signature, signatureMinted) }

// sandForwardsReasoningSignature reports whether a history signature should be
// sent to Cursor: only signatures Cursor minted through this sidecar qualify.
func sandForwardsReasoningSignature(signature string) bool {
	if !sandReasoningSignatureForwarding || strings.TrimSpace(signature) == "" {
		return false
	}
	return signatureMemory.lookup(signature) == signatureMinted
}

func sandForwardsRedactedReasoning() bool { return sandRedactedReasoningForwarding }

// historySignatures returns every reasoning signature in structured history.
func historySignatures(messages [][]byte) []string {
	var out []string
	for _, raw := range messages {
		var message map[string]any
		if jsonx.Unmarshal(raw, &message) != nil {
			continue
		}
		parts, _ := message["content"].([]any)
		for _, rawPart := range parts {
			part, _ := rawPart.(map[string]any)
			if strings.TrimSpace(stringValue(part["type"])) == "reasoning" {
				if signature := strings.TrimSpace(stringValue(part["signature"])); signature != "" {
					out = append(out, signature)
				}
			}
		}
	}
	return out
}

// historyCarriesForwardedSignature reports whether the first attempt would
// actually have sent a signature upstream (i.e. a retry can change anything).
func historyCarriesForwardedSignature(messages [][]byte) bool {
	for _, signature := range historySignatures(messages) {
		if sandForwardsReasoningSignature(signature) {
			return true
		}
	}
	return false
}

// markHistorySignaturesRejected records every forwarded signature in the
// history as rejected so later turns skip it without another round trip.
func markHistorySignaturesRejected(messages [][]byte) {
	for _, signature := range historySignatures(messages) {
		if signatureMemory.lookup(signature) == signatureMinted {
			signatureMemory.record(signature, signatureRejected)
		}
	}
}

// isForeignReasoningSignatureRejection matches the upstream shape observed in
// A/B case B: a Connect resource_exhausted (or invalid_argument) before any
// stream part was emitted.
func isForeignReasoningSignatureRejection(err error) bool {
	var connect *connectError
	if !errors.As(err, &connect) {
		return false
	}
	code := strings.ToLower(strings.TrimSpace(connect.Code))
	return code == "resource_exhausted" || code == "invalid_argument"
}
