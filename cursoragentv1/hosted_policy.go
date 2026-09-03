package cursoragentv1

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/url"
	"strings"
)

const (
	hostedServerWebSearchType = "web_search_20250305"
	hostedServerWebFetchType  = "web_fetch_20250305"
)

type hostedRunPolicy struct {
	AllowSearch   bool
	AllowFetch    bool
	RequireSearch bool
	RequireFetch  bool
	RequireAny    bool
	SearchMaxUses *int
	FetchMaxUses  *int
	// Domain restrictions declared on the Anthropic server tool. They are
	// enforced locally on the Sand data plane by filtering results before the
	// model sees them (see hostedDomainPolicy); Agent v1 cannot honor them.
	SearchDomains hostedDomainPolicy
	FetchDomains  hostedDomainPolicy
}

func (p hostedRunPolicy) enabled() bool {
	return p.AllowSearch || p.AllowFetch
}

func (p hostedRunPolicy) restrictsDomains() bool {
	return p.SearchDomains.restricts() || p.FetchDomains.restricts()
}

type hostedDeclaredTool struct {
	Kind    string
	Name    string
	MaxUses *int
	Domains hostedDomainPolicy
}

// hostedDomainPolicy mirrors Anthropic allowed_domains / blocked_domains: a
// bare host also matches its subdomains, and an entry with a path only matches
// URLs under that path prefix. Anthropic rejects declaring both lists.
type hostedDomainPolicy struct {
	Allowed []string `json:"allowed,omitempty"`
	Blocked []string `json:"blocked,omitempty"`
}

func (p hostedDomainPolicy) restricts() bool {
	return len(p.Allowed) > 0 || len(p.Blocked) > 0
}

func (p hostedDomainPolicy) permits(rawURL string) bool {
	if !p.restricts() {
		return true
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Hostname() == "" {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	path := parsed.EscapedPath()
	matches := func(rule string) bool {
		rule = strings.ToLower(strings.TrimSpace(rule))
		rule = strings.TrimPrefix(strings.TrimPrefix(rule, "https://"), "http://")
		ruleHost, rulePath, _ := strings.Cut(rule, "/")
		ruleHost = strings.TrimPrefix(ruleHost, "www.")
		candidate := strings.TrimPrefix(host, "www.")
		if ruleHost == "" {
			return false
		}
		if candidate != ruleHost && !strings.HasSuffix(candidate, "."+ruleHost) {
			return false
		}
		if rulePath == "" {
			return true
		}
		return strings.HasPrefix(strings.TrimPrefix(path, "/"), strings.TrimSuffix(rulePath, "/"))
	}
	for _, rule := range p.Blocked {
		if matches(rule) {
			return false
		}
	}
	if len(p.Allowed) == 0 {
		return true
	}
	for _, rule := range p.Allowed {
		if matches(rule) {
			return true
		}
	}
	return false
}

func (p hostedDomainPolicy) merge(next hostedDomainPolicy) hostedDomainPolicy {
	return hostedDomainPolicy{
		Allowed: appendUniqueStrings(p.Allowed, next.Allowed...),
		Blocked: appendUniqueStrings(p.Blocked, next.Blocked...),
	}
}

func appendUniqueStrings(base []string, values ...string) []string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		duplicate := false
		for _, existing := range base {
			if existing == value {
				duplicate = true
				break
			}
		}
		if !duplicate {
			base = append(base, value)
		}
	}
	return base
}

// knownServerWebKind classifies Anthropic server web tools by family. Version
// suffixes are dated releases of the same capability, so any web_search_* /
// web_fetch_* version maps to the kind (Kiro and the Anthropic relay match by
// prefix too); unknown fields are still validated in parseHostedDeclaredTool.
func knownServerWebKind(typeName string) string {
	typeName = strings.ToLower(strings.TrimSpace(typeName))
	switch {
	case strings.HasPrefix(typeName, "web_search_"):
		return "search"
	case strings.HasPrefix(typeName, "web_fetch_"):
		return "fetch"
	default:
		return ""
	}
}

func parseHostedDeclaredTool(tool map[string]any, kind string) (hostedDeclaredTool, error) {
	declared := hostedDeclaredTool{Kind: kind, Name: stringValue(tool["name"])}
	if declared.Name == "" {
		if kind == "search" {
			declared.Name = "web_search"
		} else {
			declared.Name = "web_fetch"
		}
	}
	for key, value := range tool {
		switch key {
		case "type", "name", "input_schema", "cache_control":
		case "user_location", "citations", "max_content_tokens":
			// Hints, not restrictions: Kiro and the Anthropic relay forward them
			// without effect. Cursor's hosted web has no equivalent knob.
		case "max_uses":
			maxUses, err := nonNegativeIntFromJSON(value)
			if err != nil {
				return hostedDeclaredTool{}, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 hosted search max_uses must be a non-negative integer"}
			}
			declared.MaxUses = maxUses
		case "allowed_domains", "blocked_domains":
			domains, ok := domainListFromJSON(value)
			if !ok {
				return hostedDeclaredTool{}, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 hosted search " + key + " must be a list of domains"}
			}
			if key == "allowed_domains" {
				declared.Domains.Allowed = domains
			} else {
				declared.Domains.Blocked = domains
			}
		default:
			return hostedDeclaredTool{}, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 hosted search cannot faithfully apply extra server-tool fields"}
		}
	}
	if len(declared.Domains.Allowed) > 0 && len(declared.Domains.Blocked) > 0 {
		return hostedDeclaredTool{}, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 hosted search cannot combine allowed_domains and blocked_domains"}
	}
	return declared, nil
}

func summarizeHostedPolicy(tools []hostedDeclaredTool, requireDeclared bool) hostedRunPolicy {
	var policy hostedRunPolicy
	var searchMax, fetchMax *int
	searchCount, fetchCount := 0, 0
	for _, tool := range tools {
		switch tool.Kind {
		case "search":
			policy.AllowSearch = true
			searchCount++
			searchMax = minMaxUses(searchMax, tool.MaxUses)
			policy.SearchDomains = policy.SearchDomains.merge(tool.Domains)
		case "fetch":
			policy.AllowFetch = true
			fetchCount++
			fetchMax = minMaxUses(fetchMax, tool.MaxUses)
			policy.FetchDomains = policy.FetchDomains.merge(tool.Domains)
		}
	}
	policy.SearchMaxUses = searchMax
	policy.FetchMaxUses = fetchMax
	if !requireDeclared {
		return policy
	}
	if searchCount > 0 && fetchCount == 0 {
		policy.RequireSearch = true
	} else if fetchCount > 0 && searchCount == 0 {
		policy.RequireFetch = true
	} else if searchCount > 0 && fetchCount > 0 {
		policy.RequireAny = true
	}
	return policy
}

func minMaxUses(current, next *int) *int {
	if next == nil {
		return current
	}
	if current == nil || *next < *current {
		copied := *next
		return &copied
	}
	return current
}

func domainListFromJSON(value any) ([]string, bool) {
	if value == nil {
		return nil, true
	}
	list, ok := value.([]any)
	if !ok {
		return nil, false
	}
	domains := make([]string, 0, len(list))
	for _, item := range list {
		text, ok := item.(string)
		if !ok {
			return nil, false
		}
		if text = strings.TrimSpace(text); text != "" {
			domains = append(domains, text)
		}
	}
	return domains, true
}

func nonNegativeIntFromJSON(value any) (*int, error) {
	if value == nil {
		return nil, nil
	}
	var number int64
	switch typed := value.(type) {
	case int:
		number = int64(typed)
	case int32:
		number = int64(typed)
	case int64:
		number = typed
	case float64:
		if math.Trunc(typed) != typed || typed < 0 || typed > float64(math.MaxInt) {
			return nil, errHostedMaxUses
		}
		number = int64(typed)
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return nil, errHostedMaxUses
		}
		number = parsed
	default:
		return nil, errHostedMaxUses
	}
	if number < 0 || number > int64(math.MaxInt) {
		return nil, errHostedMaxUses
	}
	parsed := int(number)
	return &parsed, nil
}

var errHostedMaxUses = errors.New("hosted max_uses must be a non-negative integer")
