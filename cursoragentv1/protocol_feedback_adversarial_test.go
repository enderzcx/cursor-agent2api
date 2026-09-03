package cursoragentv1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFeedbackCannotHideUnknownBehindKnownTerminal(t *testing.T) {
	update := appendBytesField(nil, 21, appendStringField(nil, 1, "synthetic-feedback"))
	update = appendBytesField(update, 42, nil)
	update = appendBytesField(update, 14, nil)
	_, err := decodeServerMessage(appendBytesField(nil, agentServerInteractionUpdate, update))
	require.ErrorContains(t, err, "unknown field(s) [42]")
}

func TestFeedbackRejectsMalformedNestedCategory(t *testing.T) {
	group := appendBytesField(nil, 3, []byte{0xff})
	feedback := appendBytesField(nil, 4, group)
	_, err := decodeServerMessage(appendBytesField(nil, agentServerInteractionUpdate, appendBytesField(nil, 21, feedback)))
	require.Error(t, err)
}
