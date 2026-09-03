package cursoragentv1

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
)

const (
	inferenceWebSearchToolName = "beefapi_cursor_web_search"
	inferenceWebFetchToolName  = "beefapi_cursor_web_fetch"
	inferenceWebSearchPath     = "/aiserver.v1.AiService/RunWebSearch"
	inferenceWebFetchPath      = "/aiserver.v1.AiService/RunWebFetch"
	maxInferenceWebBodyBytes   = 4 << 20
	maxInferenceWebResultRunes = 30000
	maxInferenceHostedSteps    = 16
	maxInferenceHostedUses     = 8
)

type inferenceHostedClient interface {
	Search(context.Context, RunRequest, string, string) (*HostedSearch, any, error)
	Fetch(context.Context, RunRequest, string) (*HostedSearch, any, error)
}

type cursorInferenceHostedClient struct {
	baseURL string
	client  *http.Client
}

type inferenceHostedUseTracker struct {
	search int
	fetch  int
}

type cursorInferenceSearchResponse struct {
	Answer    string `json:"answer"`
	Documents []struct {
		URL   string `json:"url"`
		Title string `json:"title"`
		Text  string `json:"text"`
	} `json:"documents"`
}

type cursorInferenceFetchResponse struct {
	Success *struct {
		Content string `json:"content"`
	} `json:"success"`
	Error *struct {
		Error     string `json:"error"`
		IsTimeout bool   `json:"isTimeout"`
	} `json:"error"`
}

func newCursorInferenceHostedClient() inferenceHostedClient {
	return &cursorInferenceHostedClient{baseURL: defaultInferenceBaseURL}
}

func attachInferenceHostedTools(run *RunRequest) error {
	if run == nil || (!run.AllowHostedSearch && !run.AllowHostedFetch) {
		return nil
	}
	for _, tool := range run.Tools {
		if inferenceHostedKind(tool.Name) != "" {
			return &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Sand caller tool uses a reserved hosted-web name"}
		}
	}
	allowSearch := run.AllowHostedSearch && (run.HostedSearchMaxUses == nil || *run.HostedSearchMaxUses > 0)
	allowFetch := run.AllowHostedFetch && (run.HostedFetchMaxUses == nil || *run.HostedFetchMaxUses > 0)
	if run.RequireHostedSearch && !allowSearch || run.RequireHostedFetch && !allowFetch || run.RequireHostedTool && !allowSearch && !allowFetch {
		return &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Sand required hosted web tool has max_uses=0"}
	}
	if allowSearch {
		schema, err := jsonx.Marshal(map[string]any{
			"type": "object",
			"properties": map[string]any{
				"search_term": map[string]any{"type": "string", "description": "Specific web search query."},
				"explanation": map[string]any{"type": "string", "description": "Why this search helps answer the user."},
			},
			"required":             []string{"search_term"},
			"additionalProperties": false,
		})
		if err != nil {
			return err
		}
		run.Tools = append(run.Tools, ToolDefinition{
			Name:        inferenceWebSearchToolName,
			Description: "Search the live web through Cursor's official web search service. Return current sources before answering time-sensitive or explicitly researched questions.",
			InputSchema: schema,
		})
	}
	if allowFetch {
		schema, err := jsonx.Marshal(map[string]any{
			"type": "object",
			"properties": map[string]any{
				"url": map[string]any{"type": "string", "description": "Fully-qualified public HTTP or HTTPS URL to fetch."},
			},
			"required":             []string{"url"},
			"additionalProperties": false,
		})
		if err != nil {
			return err
		}
		run.Tools = append(run.Tools, ToolDefinition{
			Name:        inferenceWebFetchToolName,
			Description: "Fetch readable content for a public HTTP or HTTPS URL through Cursor's official web fetch service.",
			InputSchema: schema,
		})
	}
	required := ""
	switch {
	case run.RequireHostedSearch:
		required = "You must call " + inferenceWebSearchToolName + " before answering this request."
	case run.RequireHostedFetch:
		required = "You must call " + inferenceWebFetchToolName + " before answering this request."
	case run.RequireHostedTool:
		required = "You must call " + inferenceWebSearchToolName + " or " + inferenceWebFetchToolName + " before answering this request."
	}
	if required != "" {
		if strings.TrimSpace(run.SystemText) != "" {
			run.SystemText += "\n\n"
		}
		run.SystemText += required
	}
	return nil
}

func inferenceHostedKind(name string) HostedSearchKind {
	switch strings.TrimSpace(name) {
	case inferenceWebSearchToolName:
		return HostedSearchKindWebSearch
	case inferenceWebFetchToolName:
		return HostedSearchKindWebFetch
	default:
		return ""
	}
}

func (c *cursorInferenceHostedClient) Search(ctx context.Context, request RunRequest, query, explanation string) (*HostedSearch, any, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, nil, fmt.Errorf("Cursor Sand web search query is empty")
	}
	modelID, _, err := inferenceModelSelection(request.Model)
	if err != nil {
		return nil, nil, err
	}
	var response cursorInferenceSearchResponse
	err = c.post(ctx, request, inferenceWebSearchPath, map[string]any{
		"searchTerm":  query,
		"explanation": strings.TrimSpace(explanation),
		"modelId":     modelID,
	}, &response)
	search := &HostedSearch{Kind: HostedSearchKindWebSearch, Query: query}
	if err != nil {
		search.Error = "Cursor official web search was unavailable"
		return search, map[string]any{"error": "Cursor official web search was unavailable"}, nil
	}
	domains := request.HostedSearchDomains
	restricted := domains.restricts()
	documents := make([]any, 0, len(response.Documents))
	for _, document := range response.Documents {
		if restricted && !domains.permits(document.URL) {
			continue
		}
		text := truncateInferenceWebText(document.Text)
		search.References = append(search.References, HostedSearchReference{Title: document.Title, URL: document.URL, Chunk: text})
		documents = append(documents, map[string]any{"url": document.URL, "title": document.Title, "text": text})
	}
	if restricted {
		// Cursor's synthesized answer draws on every document, including the
		// ones just filtered out, so only the permitted sources reach the model.
		result := map[string]any{"documents": documents, "domain_filter": domains}
		if len(documents) == 0 {
			result["note"] = "no results matched the caller's domain restriction"
		}
		return search, result, nil
	}
	search.Content = truncateInferenceWebText(response.Answer)
	return search, map[string]any{"answer": search.Content, "documents": documents}, nil
}

func (c *cursorInferenceHostedClient) Fetch(ctx context.Context, request RunRequest, rawURL string) (*HostedSearch, any, error) {
	rawURL = strings.TrimSpace(rawURL)
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, nil, fmt.Errorf("Cursor Sand web fetch URL must be public HTTP or HTTPS")
	}
	if parsed.User != nil {
		return nil, nil, fmt.Errorf("Cursor Sand web fetch URL must not contain credentials")
	}
	if !request.HostedFetchDomains.permits(rawURL) {
		// Same outcome Anthropic reports: the fetch is refused, the model is
		// told why, and the turn continues.
		fetch := &HostedSearch{Kind: HostedSearchKindWebFetch, Query: rawURL, URL: rawURL, Error: "URL is outside the caller's allowed domains"}
		return fetch, map[string]any{"error": fetch.Error, "domain_filter": request.HostedFetchDomains}, nil
	}
	var response cursorInferenceFetchResponse
	err = c.post(ctx, request, inferenceWebFetchPath, map[string]any{"url": rawURL}, &response)
	fetch := &HostedSearch{Kind: HostedSearchKindWebFetch, Query: rawURL, URL: rawURL}
	if err != nil {
		fetch.Error = "Cursor official web fetch was unavailable"
		return fetch, map[string]any{"error": "Cursor official web fetch was unavailable"}, nil
	}
	if response.Success != nil {
		fetch.Content = truncateInferenceWebText(response.Success.Content)
		return fetch, map[string]any{"content": fetch.Content}, nil
	}
	if response.Error != nil {
		fetch.Error = strings.TrimSpace(response.Error.Error)
		if fetch.Error == "" {
			fetch.Error = "Cursor official web fetch returned an error"
		}
		return fetch, map[string]any{"error": fetch.Error, "is_timeout": response.Error.IsTimeout}, nil
	}
	fetch.Error = "Cursor official web fetch returned no result"
	return fetch, map[string]any{"error": fetch.Error}, nil
}

func (c *cursorInferenceHostedClient) post(ctx context.Context, request RunRequest, path string, input, output any) error {
	body, err := jsonx.Marshal(input)
	if err != nil {
		return err
	}
	baseURL := strings.TrimRight(strings.TrimSpace(c.baseURL), "/")
	if baseURL == "" {
		baseURL = defaultInferenceBaseURL
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build Cursor official web request: %w", err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+strings.TrimSpace(request.AccessToken))
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Connect-Protocol-Version", "1")
	httpRequest.Header.Set("X-Cursor-Client-Type", runtimeSand)
	httpRequest.Header.Set("X-Cursor-Client-Version", strings.TrimSpace(request.ClientVersion))
	httpRequest.Header.Set("User-Agent", "connect-es/1.6.1")
	client := c.client
	closeIdle := func() {}
	if client == nil {
		transport, transportErr := cursorHTTP2Transport(request.ProxyURL, nil)
		if transportErr != nil {
			return transportErr
		}
		client = &http.Client{Transport: transport}
		closeIdle = transport.CloseIdleConnections
	}
	response, err := client.Do(httpRequest)
	if err != nil {
		closeIdle()
		return fmt.Errorf("Cursor official web request: %w", err)
	}
	defer response.Body.Close()
	defer closeIdle()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxInferenceWebBodyBytes+1))
	if err != nil {
		return fmt.Errorf("read Cursor official web response: %w", err)
	}
	if len(responseBody) > maxInferenceWebBodyBytes {
		return fmt.Errorf("Cursor official web response exceeds %d bytes", maxInferenceWebBodyBytes)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return &httpStatusError{code: response.StatusCode, text: fmt.Sprintf("Cursor official web upstream HTTP %d", response.StatusCode)}
	}
	if err := jsonx.Unmarshal(responseBody, output); err != nil {
		return fmt.Errorf("decode Cursor official web response: %w", err)
	}
	return nil
}

func truncateInferenceWebText(value string) string {
	runes := []rune(value)
	if len(runes) <= maxInferenceWebResultRunes {
		return value
	}
	return string(runes[:maxInferenceWebResultRunes])
}

func inferenceHostedArguments(call inferenceToolCall) (string, string, error) {
	var args map[string]any
	if jsonx.Unmarshal([]byte(call.Arguments), &args) != nil {
		return "", "", fmt.Errorf("Cursor Sand hosted tool returned invalid arguments")
	}
	switch inferenceHostedKind(call.Name) {
	case HostedSearchKindWebSearch:
		query := firstInferenceValue(stringValue(args["search_term"]), stringValue(args["query"]), stringValue(args["searchTerm"]))
		return query, stringValue(args["explanation"]), nil
	case HostedSearchKindWebFetch:
		return stringValue(args["url"]), "", nil
	default:
		return "", "", fmt.Errorf("Cursor Sand hosted tool is unsupported")
	}
}

func firstInferenceValue(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func (t *inferenceHostedUseTracker) allow(kind HostedSearchKind, request RunRequest) error {
	if t == nil {
		return fmt.Errorf("Cursor Sand hosted web tracker is unavailable")
	}
	limit := maxInferenceHostedUses
	switch kind {
	case HostedSearchKindWebSearch:
		if request.HostedSearchMaxUses != nil && *request.HostedSearchMaxUses < limit {
			limit = *request.HostedSearchMaxUses
		}
		if t.search >= limit {
			return fmt.Errorf("Cursor Sand hosted search exceeded max_uses=%d", limit)
		}
		t.search++
	case HostedSearchKindWebFetch:
		if request.HostedFetchMaxUses != nil && *request.HostedFetchMaxUses < limit {
			limit = *request.HostedFetchMaxUses
		}
		if t.fetch >= limit {
			return fmt.Errorf("Cursor Sand hosted fetch exceeded max_uses=%d", limit)
		}
		t.fetch++
	default:
		return fmt.Errorf("Cursor Sand hosted web tool is unsupported")
	}
	return nil
}

func (t *inferenceHostedUseTracker) validateRequired(request RunRequest) error {
	if t == nil {
		return fmt.Errorf("Cursor Sand hosted web tracker is unavailable")
	}
	if request.RequireHostedSearch && t.search == 0 {
		return fmt.Errorf("Cursor Sand completed without the required hosted search")
	}
	if request.RequireHostedFetch && t.fetch == 0 {
		return fmt.Errorf("Cursor Sand completed without the required hosted fetch")
	}
	if request.RequireHostedTool && t.search+t.fetch == 0 {
		return fmt.Errorf("Cursor Sand completed without the required hosted web tool")
	}
	return nil
}

func executeInferenceHostedCall(ctx context.Context, client inferenceHostedClient, uses *inferenceHostedUseTracker, request RunRequest, call inferenceToolCall) (inferenceHostedToolResult, *HostedSearch, error) {
	kind := inferenceHostedKind(call.Name)
	if kind == "" || client == nil {
		return inferenceHostedToolResult{}, nil, fmt.Errorf("Cursor Sand hosted web client is unavailable")
	}
	if err := uses.allow(kind, request); err != nil {
		return inferenceHostedToolResult{}, nil, err
	}
	primary, explanation, err := inferenceHostedArguments(call)
	if err != nil {
		return inferenceHostedToolResult{}, nil, err
	}
	if kind == HostedSearchKindWebSearch && strings.TrimSpace(primary) == "" {
		primary = strings.TrimSpace(request.UserText)
	}
	var search *HostedSearch
	var content any
	if kind == HostedSearchKindWebSearch {
		search, content, err = client.Search(ctx, request, primary, explanation)
	} else {
		search, content, err = client.Fetch(ctx, request, primary)
	}
	if err != nil {
		return inferenceHostedToolResult{}, nil, err
	}
	if search == nil {
		return inferenceHostedToolResult{}, nil, fmt.Errorf("Cursor Sand hosted web returned no result")
	}
	search.CallID = call.ID
	search.ArgsJSON = call.Arguments
	return inferenceHostedToolResult{CallID: call.ID, Name: call.Name, Content: content, IsError: search.Error != ""}, search, nil
}
