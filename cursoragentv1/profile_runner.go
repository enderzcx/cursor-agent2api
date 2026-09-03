package cursoragentv1

import (
	"context"
	"fmt"
	"strings"
)

type profileRunner struct {
	agent runEngine
	sand  runEngine
}

func newProfileRunner() *profileRunner {
	return &profileRunner{agent: NewRunner(), sand: NewInferenceRunner()}
}

func (r *profileRunner) Run(ctx context.Context, request RunRequest, emit func(Event)) error {
	if r == nil {
		return fmt.Errorf("Cursor Agent v1 profile runner is not configured")
	}
	switch strings.ToLower(strings.TrimSpace(request.RuntimeProfile)) {
	case "", runtimeAgentV1:
		return r.agent.Run(ctx, request, emit)
	case runtimeSand:
		return r.sand.Run(ctx, request, emit)
	default:
		return fmt.Errorf("Cursor Agent v1 runtime profile %q is invalid", request.RuntimeProfile)
	}
}

func normalizeRuntimeProfile(profile string) string {
	if strings.EqualFold(strings.TrimSpace(profile), runtimeSand) {
		return runtimeSand
	}
	return runtimeAgentV1
}
