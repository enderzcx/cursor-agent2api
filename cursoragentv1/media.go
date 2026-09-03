package cursoragentv1

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"net/http"
	"strings"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
)

// MediaPart is a caller-supplied binary attachment (screenshot, pasted image,
// PDF) that must reach the upstream model as native media rather than as text.
// Data is standard base64 without a data: prefix so both Cursor data planes can
// encode it directly (InferenceImagePart/InferenceFilePart carry base64 strings;
// Agent v1 SelectedImage/McpImageContent carry raw bytes).
type MediaPart struct {
	Kind     string `json:"kind"` // "image" or "document"
	MimeType string `json:"mime_type"`
	Data     string `json:"data"`
	Filename string `json:"filename,omitempty"`
}

const (
	mediaKindImage    = "image"
	mediaKindDocument = "document"
	// One attachment may fill a Connect frame. Anthropic/Kiro do not invent a
	// tighter local 5 MiB check; PDFs between 5 MiB and the frame still fail
	// on type64 today. Cursor may still reject oversized media upstream.
	maxMediaPartBytes = maxConnectPayloadSize
)

// ContentPart is one element of a mixed user or tool-result payload: exactly
// one of Text or Media is populated.
type ContentPart struct {
	Text  string
	Media *MediaPart
}

func (p MediaPart) rawBytes() ([]byte, error) {
	return base64.StdEncoding.DecodeString(p.Data)
}

// imageDimensions reports width/height for PNG/JPEG/GIF so Agent v1
// SelectedImage can carry the dimension Cursor's own client always sends.
// Unknown formats return zero dimensions rather than an error.
func (p MediaPart) imageDimensions() (int, int) {
	raw, err := p.rawBytes()
	if err != nil {
		return 0, 0
	}
	config, _, err := image.DecodeConfig(bytes.NewReader(raw))
	if err != nil {
		return 0, 0
	}
	return config.Width, config.Height
}

// parseAnthropicMediaBlock converts a Claude `image` or `document` content
// block into a MediaPart. It returns (nil, nil) for blocks that are not media.
// Text documents and search results are returned as ContentPart text by
// parseAnthropicContentBlock instead; this helper only handles binary media.
func parseAnthropicMediaBlock(block map[string]any) (*MediaPart, error) {
	kind := stringValue(block["type"])
	if kind != "image" && kind != "document" {
		return nil, nil
	}
	source, _ := block["source"].(map[string]any)
	if source == nil {
		return nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 " + kind + " block has no source"}
	}
	switch stringValue(source["type"]) {
	case "base64":
		data := strings.TrimSpace(stringValue(source["data"]))
		mime := strings.ToLower(stringValue(source["media_type"]))
		if data == "" || mime == "" {
			return nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 " + kind + " block needs base64 data and media_type"}
		}
		if kind == "image" && !strings.HasPrefix(mime, "image/") {
			return nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 image block media_type must be image/*"}
		}
		decodedSize := base64.StdEncoding.DecodedLen(len(data))
		if decodedSize > maxMediaPartBytes {
			return nil, &requestError{status: http.StatusUnprocessableEntity, text: fmt.Sprintf("Cursor Agent v1 media attachment exceeds %d MiB", maxMediaPartBytes>>20)}
		}
		if _, err := base64.StdEncoding.DecodeString(data); err != nil {
			return nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 " + kind + " block base64 is invalid"}
		}
		part := &MediaPart{Kind: kind, MimeType: mime, Data: data}
		if kind == "document" {
			part.Filename = stringValue(block["title"])
		}
		return part, nil
	case "url":
		// Neither Cursor data plane fetches remote media on the caller's behalf,
		// and proxying arbitrary URLs from the sidecar would be an SSRF surface.
		return nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 " + kind + " url sources are not supported; send base64"}
	case "file":
		return nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 " + kind + " file_id sources belong to the Anthropic Files API and cannot be replayed here"}
	case "text", "content":
		return nil, nil
	default:
		return nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 " + kind + " source type is unsupported"}
	}
}

// parseAnthropicContentBlock maps the Claude content blocks that can appear in
// user messages and tool_result content to ContentParts. Unknown block types
// return ok=false so callers keep their own fail-closed handling.
func parseAnthropicContentBlock(block map[string]any) (parts []ContentPart, ok bool, err error) {
	switch stringValue(block["type"]) {
	case "text":
		text, isString := block["text"].(string)
		if !isString {
			return nil, true, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 text block has no text"}
		}
		return []ContentPart{{Text: text}}, true, nil
	case "image":
		media, mediaErr := parseAnthropicMediaBlock(block)
		if mediaErr != nil {
			return nil, true, mediaErr
		}
		return []ContentPart{{Media: media}}, true, nil
	case "document":
		media, mediaErr := parseAnthropicMediaBlock(block)
		if mediaErr != nil {
			return nil, true, mediaErr
		}
		if media != nil {
			out := []ContentPart{{Media: media}}
			if context := stringValue(block["context"]); context != "" {
				out = append(out, ContentPart{Text: "Document context: " + context})
			}
			return out, true, nil
		}
		// Plain-text or pre-chunked documents carry their own text; keep the
		// title so the model can still reference the source.
		source, _ := block["source"].(map[string]any)
		text := documentSourceText(source)
		if title := stringValue(block["title"]); title != "" {
			text = "Document \"" + title + "\":\n" + text
		}
		if context := stringValue(block["context"]); context != "" {
			text = context + "\n" + text
		}
		return []ContentPart{{Text: text}}, true, nil
	case "search_result":
		return []ContentPart{{Text: searchResultText(block)}}, true, nil
	default:
		return nil, false, nil
	}
}

func documentSourceText(source map[string]any) string {
	if source == nil {
		return ""
	}
	if data, ok := source["data"].(string); ok {
		return data
	}
	if content, ok := source["content"].([]any); ok {
		return contentText(content)
	}
	if content, ok := source["content"].(string); ok {
		return content
	}
	return ""
}

// searchResultText renders an Anthropic search_result block as text evidence.
// Cursor has no native block for caller-provided search results, so the model
// receives the same title/source/content the client supplied.
func searchResultText(block map[string]any) string {
	var builder strings.Builder
	builder.WriteString("Search result")
	if title := stringValue(block["title"]); title != "" {
		builder.WriteString(" \"" + title + "\"")
	}
	if source := stringValue(block["source"]); source != "" {
		builder.WriteString(" (" + source + ")")
	}
	builder.WriteString(":\n")
	builder.WriteString(contentText(block["content"]))
	return builder.String()
}

// splitContentParts separates text from media, joining adjacent text with
// newlines. Callers use the text for protocols that need a plain string and the
// media list for native attachment fields.
func splitContentParts(parts []ContentPart) (string, []MediaPart) {
	texts := make([]string, 0, len(parts))
	media := make([]MediaPart, 0)
	for _, part := range parts {
		if part.Media != nil {
			media = append(media, *part.Media)
			continue
		}
		if part.Text != "" {
			texts = append(texts, part.Text)
		}
	}
	return strings.Join(texts, "\n"), media
}

// historyContentParts renders ContentParts in the structured-history JSON
// shape shared by both Cursor data planes (AI SDK core message parts).
func historyContentParts(parts []ContentPart) []any {
	out := make([]any, 0, len(parts))
	for _, part := range parts {
		if part.Media != nil {
			out = append(out, historyMediaPart(*part.Media))
			continue
		}
		out = append(out, map[string]any{"type": "text", "text": part.Text})
	}
	return out
}

func historyMediaPart(media MediaPart) map[string]any {
	if media.Kind == mediaKindImage {
		return map[string]any{"type": "image", "image": media.Data, "mimeType": media.MimeType}
	}
	part := map[string]any{"type": "file", "data": media.Data, "mimeType": media.MimeType}
	if media.Filename != "" {
		part["filename"] = media.Filename
	}
	return part
}

// mediaPartFromHistory decodes the structured-history media shapes produced by
// historyMediaPart. ok=false means the part is not media.
func mediaPartFromHistory(part map[string]any) (MediaPart, bool) {
	switch strings.TrimSpace(stringValue(part["type"])) {
	case "image":
		data, _ := part["image"].(string)
		mime, _ := part["mimeType"].(string)
		if data == "" {
			return MediaPart{}, false
		}
		if mime == "" {
			mime = "image/png"
		}
		return MediaPart{Kind: mediaKindImage, MimeType: mime, Data: data}, true
	case "file":
		data, _ := part["data"].(string)
		mime, _ := part["mimeType"].(string)
		if data == "" || mime == "" {
			return MediaPart{}, false
		}
		filename, _ := part["filename"].(string)
		return MediaPart{Kind: mediaKindDocument, MimeType: mime, Data: data, Filename: filename}, true
	default:
		return MediaPart{}, false
	}
}

func cloneMedia(media []MediaPart) []MediaPart {
	if len(media) == 0 {
		return nil
	}
	return append([]MediaPart(nil), media...)
}

func mediaSummary(media []MediaPart) string {
	if len(media) == 0 {
		return ""
	}
	labels := make([]string, 0, len(media))
	for _, part := range media {
		label := part.Kind + " " + part.MimeType
		if part.Filename != "" {
			label += " \"" + part.Filename + "\""
		}
		labels = append(labels, label)
	}
	encoded, _ := jsonx.Marshal(labels)
	return string(encoded)
}
