package cursoragentv1

import (
	"fmt"

	agentv1 "github.com/enderzcx/cursor-agent2api/cursoragentv1/gen"
	"google.golang.org/protobuf/encoding/protowire"
)

// Official Cursor Agent CLI 2026.08.25-3e8eec8 descriptor:
// InteractionUpdate.feedback_request = 21 (bytes oneof).
// FeedbackRequestUpdate is a UI survey (request_id=1 string, optional
// canonical_model_name=2, categories=3, category_groups=4,
// show_form_immediately=5 bool, title=6). The CLI observer ignores
// feedbackRequest; it is not exec, query, terminal, or usage.
const interactionFeedbackRequestField protowire.Number = 21

func remainingInteractionUnknown(update *agentv1.InteractionUpdate) ([]protowire.Number, error) {
	if update == nil {
		return nil, nil
	}
	unknown := update.ProtoReflect().GetUnknown()
	var remaining []protowire.Number
	feedbackSeen := false
	seen := map[protowire.Number]struct{}{}
	for len(unknown) > 0 {
		number, wireType, consumed := protowire.ConsumeTag(unknown)
		if consumed < 0 {
			return nil, fmt.Errorf("decode Cursor Agent v1 interaction update: %v", protowire.ParseError(consumed))
		}
		unknown = unknown[consumed:]
		if number == interactionFeedbackRequestField {
			feedbackSeen = true
			if wireType != protowire.BytesType {
				return nil, fmt.Errorf("Cursor Agent v1 feedback request must be length-delimited")
			}
			value, n := protowire.ConsumeBytes(unknown)
			if n < 0 {
				return nil, fmt.Errorf("decode Cursor Agent v1 feedback request: %v", protowire.ParseError(n))
			}
			if err := validateFeedbackRequestUpdate(value); err != nil {
				return nil, err
			}
			unknown = unknown[n:]
			continue
		}
		n := protowire.ConsumeFieldValue(number, wireType, unknown)
		if n < 0 {
			return nil, fmt.Errorf("decode Cursor Agent v1 interaction update field %d: %v", number, protowire.ParseError(n))
		}
		unknown = unknown[n:]
		if _, exists := seen[number]; exists {
			continue
		}
		seen[number] = struct{}{}
		remaining = append(remaining, number)
	}
	if feedbackSeen && len(remaining) > 0 {
		return nil, protocolDriftError("interaction update", remaining)
	}
	return remaining, nil
}

func validateFeedbackRequestUpdate(data []byte) error {
	for len(data) > 0 {
		number, wireType, consumed := protowire.ConsumeTag(data)
		if consumed < 0 {
			return fmt.Errorf("decode Cursor Agent v1 feedback request: %v", protowire.ParseError(consumed))
		}
		data = data[consumed:]
		var n int
		switch number {
		case 1, 2, 6, 7, 8:
			if wireType != protowire.BytesType {
				return fmt.Errorf("Cursor Agent v1 feedback request field %d must be length-delimited", number)
			}
			_, n = protowire.ConsumeBytes(data)
		case 3, 4:
			if wireType != protowire.BytesType {
				return fmt.Errorf("Cursor Agent v1 feedback request field %d must be length-delimited", number)
			}
			inner, innerN := protowire.ConsumeBytes(data)
			if innerN < 0 {
				return fmt.Errorf("decode Cursor Agent v1 feedback request field %d: %v", number, protowire.ParseError(innerN))
			}
			if err := validateFeedbackCategory(inner, number == 4); err != nil {
				return err
			}
			n = innerN
		case 5:
			if wireType != protowire.VarintType {
				return fmt.Errorf("Cursor Agent v1 feedback request field %d must be varint", number)
			}
			_, n = protowire.ConsumeVarint(data)
		default:
			n = protowire.ConsumeFieldValue(number, wireType, data)
		}
		if n < 0 {
			return fmt.Errorf("decode Cursor Agent v1 feedback request field %d: %v", number, protowire.ParseError(n))
		}
		data = data[n:]
	}
	return nil
}

// FeedbackCategoryGroup.categories (field 3) contains FeedbackCategory messages.
// Recurse only through that fixed schema edge, never through arbitrary bytes.
func validateFeedbackCategory(data []byte, group bool) error {
	for len(data) > 0 {
		number, wireType, consumed := protowire.ConsumeTag(data)
		if consumed < 0 {
			return fmt.Errorf("decode Cursor Agent v1 feedback request: %v", protowire.ParseError(consumed))
		}
		data = data[consumed:]
		var n int
		if number == 1 || number == 2 || (group && number == 3) {
			if wireType != protowire.BytesType {
				return fmt.Errorf("Cursor Agent v1 feedback category field %d must be length-delimited", number)
			}
			var value []byte
			value, n = protowire.ConsumeBytes(data)
			if n >= 0 && group && number == 3 {
				if err := validateFeedbackCategory(value, false); err != nil {
					return err
				}
			}
		} else {
			n = protowire.ConsumeFieldValue(number, wireType, data)
		}
		if n < 0 {
			return fmt.Errorf("decode Cursor Agent v1 feedback request field %d: %v", number, protowire.ParseError(n))
		}
		data = data[n:]
	}
	return nil
}
