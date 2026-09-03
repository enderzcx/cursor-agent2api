package cursoragentv1

// DefaultModels is the Agent v1 catalog this provider accepts for the default
// runtime profile. It mirrors the calibrated list BeefAPI type 64 publishes.
var DefaultModels = []string{
	"claude-fable-5-1",
	"claude-fable-5",
	"claude-opus-5",
	"claude-opus-4-6",
	"claude-opus-4-7",
	"claude-opus-4-8",
	"claude-sonnet-4-6",
	"claude-sonnet-5",
	"composer-2.5-fast",
	"glm-5.2",
	"kimi-k3",
}

// SandModels are only reachable through the Sand inference runtime, selected
// per credential with "runtime_profile": "sand" in the auth file.
var SandModels = []string{
	"grok-4.6",
}

// ModelsForRuntimeProfile returns the catalog for one credential's runtime profile.
func ModelsForRuntimeProfile(profile string) []string {
	switch profile {
	case runtimeSand:
		return append(append([]string(nil), DefaultModels...), SandModels...)
	default:
		return append([]string(nil), DefaultModels...)
	}
}
