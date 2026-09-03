package cursoragentv1

import "github.com/enderzcx/cursor-agent2api/internal/helps"

func (p *claudeProjection) outputTokens() int64 {
	if !p.facts.measuredEnabled {
		return p.facts.publicOutputTokens()
	}
	if usage := p.facts.turnEnded; p.facts.freshTerminal && usage != nil && usage.Output.Present && !usage.Output.Invalid && usage.Output.Value >= 0 {
		return usage.Output.Value
	}
	// Reuse CPA's tokenizer fallback for actually emitted text/arguments. Native
	// tokenDelta may include fetched source text and is not an output estimate.
	codec, err := helps.TokenizerForModel(p.model)
	if err != nil {
		return 0
	}
	count, err := codec.Count(p.facts.visibleOutput.String())
	if err != nil {
		return 0
	}
	if prior := p.facts.turnEnded; !p.facts.freshTerminal && prior != nil && prior.Output.Present && !prior.Output.Invalid && prior.Output.Value >= 0 {
		return prior.Output.Value + int64(count)
	}
	return int64(count)
}

func (p *claudeProjection) estimatedInputTokens() int64 {
	count, err := helps.CountClaudeInputTokens(p.request)
	if err != nil {
		return 0
	}
	return count
}
