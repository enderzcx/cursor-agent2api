package cursoragentv1

import (
	"fmt"
	"strings"

	agentv1 "github.com/enderzcx/cursor-agent2api/cursoragentv1/gen"
	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	toolClassIgnored toolClass = iota
	toolClassMCP
	toolClassHostedSearch
	toolClassUnknown
)

type toolClass int

func protocolDriftError(surface string, numbers []protowire.Number) error {
	if len(numbers) == 0 {
		return fmt.Errorf("Cursor Agent v1 protocol drift in %s", surface)
	}
	return fmt.Errorf("Cursor Agent v1 protocol drift in %s: unknown field(s) %v", surface, numbers)
}

func decodeServerMessage(data []byte) (*decodedWireMessage, error) {
	message := &decodedWireMessage{Type: wireUnknown}
	for len(data) > 0 {
		number, wireType, consumed := protowire.ConsumeTag(data)
		if consumed < 0 {
			return nil, fmt.Errorf("decode Cursor Agent v1 server tag: %v", protowire.ParseError(consumed))
		}
		data = data[consumed:]
		if wireType != protowire.BytesType {
			consumed = protowire.ConsumeFieldValue(number, wireType, data)
			if consumed < 0 {
				return nil, fmt.Errorf("decode Cursor Agent v1 server field %d: %v", number, protowire.ParseError(consumed))
			}
			data = data[consumed:]
			continue
		}
		value, consumed := protowire.ConsumeBytes(data)
		if consumed < 0 {
			return nil, fmt.Errorf("decode Cursor Agent v1 server bytes %d: %v", number, protowire.ParseError(consumed))
		}
		data = data[consumed:]
		if err := applyServerBytesField(message, number, value); err != nil {
			return nil, err
		}
	}
	return message, nil
}

func applyServerBytesField(message *decodedWireMessage, number protowire.Number, value []byte) error {
	switch number {
	case agentServerInteractionUpdate:
		if wireReplyRequired(message.Type) {
			return nil
		}
		return decodeTypedInteractionUpdate(value, message)
	case agentServerExecMessage:
		return decodeTypedExecMessage(value, message)
	case agentServerCheckpoint:
		if wireReplyRequired(message.Type) {
			return nil
		}
		message.Type = wireCheckpoint
		message.Checkpoint = append([]byte(nil), value...)
		return nil
	case agentServerKVMessage:
		return decodeTypedKVMessage(value, message)
	case 5:
		return decodeTypedExecControl(value, message)
	case agentServerInteractionQuery:
		return decodeTypedInteractionQuery(value, message)
	default:
		// Protobuf envelope extensions are forward-compatible notifications.
		// Ignoring one grants no execution and never produces a terminal event;
		// known exec/query/tool payloads retain their strict capability checks.
		return nil
	}
}

func decodeTypedInteractionUpdate(data []byte, message *decodedWireMessage) error {
	update := &agentv1.InteractionUpdate{}
	if err := proto.Unmarshal(data, update); err != nil {
		return fmt.Errorf("decode Cursor Agent v1 interaction update: %w", err)
	}
	remainingUnknown, err := remainingInteractionUnknown(update)
	if err != nil {
		return err
	}
	switch typed := update.GetMessage().(type) {
	case *agentv1.InteractionUpdate_TextDelta:
		message.Type = wireTextDelta
		message.Text = typed.TextDelta.GetText()
	case *agentv1.InteractionUpdate_ThinkingDelta:
		message.Type = wireThinkingDelta
		message.Text = typed.ThinkingDelta.GetText()
	case *agentv1.InteractionUpdate_ThinkingCompleted:
		message.Type = wireThinkingCompleted
	case *agentv1.InteractionUpdate_TokenDelta:
		message.Type = wireTokenDelta
		message.Usage = observeTokenDeltaUpdate(typed.TokenDelta)
		if message.Usage != nil {
			message.TokenDelta = message.Usage.TokenDelta.Value
		}
	case *agentv1.InteractionUpdate_TurnEnded:
		message.Type = wireTurnEnded
		message.Usage = observeTurnEndedUpdate(typed.TurnEnded)
	case *agentv1.InteractionUpdate_ToolCallStarted:
		return applyToolCallUpdate(message, wireToolCallStarted, typed.ToolCallStarted.GetCallId(), typed.ToolCallStarted.GetToolCall())
	case *agentv1.InteractionUpdate_ToolCallCompleted:
		return applyToolCallUpdate(message, wireToolCallCompleted, typed.ToolCallCompleted.GetCallId(), typed.ToolCallCompleted.GetToolCall())
	case *agentv1.InteractionUpdate_ToolCallDelta:
		return applyToolCallUpdate(message, wireToolCallDelta, typed.ToolCallDelta.GetCallId(), nil)
	case *agentv1.InteractionUpdate_PartialToolCall:
		return applyToolCallUpdate(message, wireToolCallDelta, typed.PartialToolCall.GetCallId(), typed.PartialToolCall.GetToolCall())
	case *agentv1.InteractionUpdate_Heartbeat, *agentv1.InteractionUpdate_Summary,
		*agentv1.InteractionUpdate_SummaryStarted, *agentv1.InteractionUpdate_SummaryCompleted,
		*agentv1.InteractionUpdate_UserMessageAppended, *agentv1.InteractionUpdate_ShellOutputDelta,
		*agentv1.InteractionUpdate_StepStarted, *agentv1.InteractionUpdate_StepCompleted:
		return nil
	case nil:
		if len(remainingUnknown) > 0 {
			return protocolDriftError("interaction update", remainingUnknown)
		}
		return nil
	default:
		return protocolDriftError("interaction update", []protowire.Number{oneofFieldNumber(update, update.GetMessage())})
	}
	return nil
}

func applyToolCallUpdate(message *decodedWireMessage, messageType wireMessageType, callID string, tool *agentv1.ToolCall) error {
	class, search, err := classifyToolCall(tool)
	if err != nil {
		return err
	}
	message.CallID = strings.TrimSpace(callID)
	if search != nil && search.CallID == "" {
		search.CallID = message.CallID
	}
	if message.CallID == "" && search != nil {
		message.CallID = search.CallID
	}
	switch class {
	case toolClassHostedSearch:
		message.Type = messageType
		message.HostedSearch = search
		return nil
	case toolClassMCP:
		if messageType == wireToolCallCompleted {
			// This is a transcript notification after execution, not a new
			// request. ExecServerMessage alone owns caller-tool dispatch.
			message.Type = wireIgnored
			return nil
		}
		message.Type = wireStreamMCP
		if tool != nil {
			decodeTypedMCPArgs(tool.GetMcpToolCall().GetArgs(), message)
		}
		if strings.TrimSpace(message.ToolCallID) == "" {
			message.ToolCallID = message.CallID
		}
		return nil
	case toolClassIgnored:
		message.Type = wireIgnored
		return nil
	default:
		if tool == nil {
			message.Type = wireIgnored
			return nil
		}
		unknown := filterToolCallUnknown(unknownFieldNumbers(tool))
		if len(unknown) > 0 {
			return protocolDriftError("tool call", unknown)
		}
		message.Type = wireIgnored
		return nil
	}
}

func classifyToolCall(tool *agentv1.ToolCall) (toolClass, *HostedSearch, error) {
	if tool == nil {
		return toolClassIgnored, nil, nil
	}
	switch typed := tool.GetTool().(type) {
	case *agentv1.ToolCall_WebSearchToolCall:
		search := &HostedSearch{Kind: HostedSearchKindWebSearch}
		if args := typed.WebSearchToolCall.GetArgs(); args != nil {
			search.CallID = strings.TrimSpace(args.GetToolCallId())
			search.Query = args.GetSearchTerm()
			search.ArgsJSON = marshalSearchArgs(map[string]any{"query": args.GetSearchTerm()})
		}
		if result := typed.WebSearchToolCall.GetResult(); result != nil {
			applyWebSearchResult(search, result)
		}
		if id := strings.TrimSpace(tool.GetToolCallId()); id != "" && search.CallID == "" {
			search.CallID = id
		}
		return toolClassHostedSearch, search, nil
	case *agentv1.ToolCall_WebFetchToolCall:
		return toolClassHostedSearch, hostedFetchSearch(typed.WebFetchToolCall, tool.GetToolCallId()), nil
	case *agentv1.ToolCall_FetchToolCall:
		return toolClassHostedSearch, hostedFetchSearch(typed.FetchToolCall, tool.GetToolCallId()), nil
	case *agentv1.ToolCall_ExaSearchToolCall:
		search := &HostedSearch{Kind: HostedSearchKindExaSearch}
		if args := typed.ExaSearchToolCall.GetArgs(); args != nil {
			search.CallID = strings.TrimSpace(args.GetToolCallId())
			search.Query = args.GetQuery()
			search.ArgsJSON = marshalSearchArgs(map[string]any{"query": args.GetQuery()})
		}
		if result := typed.ExaSearchToolCall.GetResult(); result != nil {
			applyExaSearchResult(search, result)
		}
		if id := strings.TrimSpace(tool.GetToolCallId()); id != "" && search.CallID == "" {
			search.CallID = id
		}
		return toolClassHostedSearch, search, nil
	case *agentv1.ToolCall_ExaFetchToolCall:
		search := &HostedSearch{Kind: HostedSearchKindExaFetch}
		if args := typed.ExaFetchToolCall.GetArgs(); args != nil {
			search.CallID = strings.TrimSpace(args.GetToolCallId())
			if len(args.GetIds()) > 0 {
				search.URL = args.GetIds()[0]
			}
			search.ArgsJSON = marshalSearchArgs(map[string]any{"ids": args.GetIds()})
		}
		if result := typed.ExaFetchToolCall.GetResult(); result != nil {
			applyExaFetchResult(search, result)
		}
		if id := strings.TrimSpace(tool.GetToolCallId()); id != "" && search.CallID == "" {
			search.CallID = id
		}
		return toolClassHostedSearch, search, nil
	case *agentv1.ToolCall_McpToolCall:
		return toolClassMCP, nil, nil
	case nil:
		unknown := filterToolCallUnknown(unknownFieldNumbers(tool))
		if len(unknown) > 0 {
			return toolClassUnknown, nil, protocolDriftError("tool call", unknown)
		}
		return toolClassIgnored, nil, nil
	default:
		return toolClassIgnored, nil, nil
	}
}

func hostedFetchSearch(call *agentv1.FetchToolCall, envelopeID string) *HostedSearch {
	search := &HostedSearch{Kind: HostedSearchKindWebFetch}
	if call == nil {
		return search
	}
	if args := call.GetArgs(); args != nil {
		search.CallID = strings.TrimSpace(args.GetToolCallId())
		search.URL = args.GetUrl()
		search.Query = args.GetUrl()
		search.ArgsJSON = marshalSearchArgs(map[string]any{"url": args.GetUrl()})
	}
	if result := call.GetResult(); result != nil {
		applyFetchResult(search, result)
	}
	if id := strings.TrimSpace(envelopeID); id != "" && search.CallID == "" {
		search.CallID = id
	}
	return search
}

func applyWebSearchResult(search *HostedSearch, result *agentv1.WebSearchResult) {
	switch typed := result.GetResult().(type) {
	case *agentv1.WebSearchResult_Success:
		for _, ref := range typed.Success.GetReferences() {
			search.References = append(search.References, HostedSearchReference{
				Title: ref.GetTitle(), URL: ref.GetUrl(), Chunk: ref.GetChunk(),
			})
		}
		search.References = expandHostedSearchLinkList(search.References)
	case *agentv1.WebSearchResult_Error:
		search.Error = typed.Error.GetError()
	case *agentv1.WebSearchResult_Rejected:
		search.Error = typed.Rejected.GetReason()
	}
}

func applyExaSearchResult(search *HostedSearch, result *agentv1.ExaSearchResult) {
	switch typed := result.GetResult().(type) {
	case *agentv1.ExaSearchResult_Success:
		for _, ref := range typed.Success.GetReferences() {
			search.References = append(search.References, HostedSearchReference{
				Title: ref.GetTitle(), URL: ref.GetUrl(), Chunk: ref.GetText(),
			})
		}
	case *agentv1.ExaSearchResult_Error:
		search.Error = typed.Error.GetError()
	case *agentv1.ExaSearchResult_Rejected:
		search.Error = typed.Rejected.GetReason()
	}
}

func applyExaFetchResult(search *HostedSearch, result *agentv1.ExaFetchResult) {
	switch typed := result.GetResult().(type) {
	case *agentv1.ExaFetchResult_Success:
		var parts []string
		for _, content := range typed.Success.GetContents() {
			search.References = append(search.References, HostedSearchReference{
				Title: content.GetTitle(), URL: content.GetUrl(), Chunk: content.GetText(),
			})
			if search.URL == "" {
				search.URL = content.GetUrl()
			}
			if text := strings.TrimSpace(content.GetText()); text != "" {
				parts = append(parts, text)
			}
		}
		search.Content = strings.Join(parts, "\n\n")
	case *agentv1.ExaFetchResult_Error:
		search.Error = typed.Error.GetError()
	case *agentv1.ExaFetchResult_Rejected:
		search.Error = typed.Rejected.GetReason()
	}
}

func applyFetchResult(search *HostedSearch, result *agentv1.FetchResult) {
	switch typed := result.GetResult().(type) {
	case *agentv1.FetchResult_Success:
		if search.URL == "" {
			search.URL = typed.Success.GetUrl()
		}
		search.Content = typed.Success.GetContent()
		if search.URL != "" {
			search.References = append(search.References, HostedSearchReference{URL: search.URL, Chunk: typed.Success.GetContent()})
		}
	case *agentv1.FetchResult_Error:
		search.Error = typed.Error.GetError()
		if search.URL == "" {
			search.URL = typed.Error.GetUrl()
		}
	}
}

func marshalSearchArgs(value map[string]any) string {
	encoded, err := jsonx.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

func decodeTypedExecMessage(data []byte, message *decodedWireMessage) error {
	if err := rejectOverflowID(data, "exec"); err != nil {
		return err
	}
	exec := &agentv1.ExecServerMessage{}
	if err := proto.Unmarshal(data, exec); err != nil {
		return fmt.Errorf("decode Cursor Agent v1 exec: %w", err)
	}
	message.ID = exec.GetId()
	message.ExecID = exec.GetExecId()
	switch exec.GetMessage().(type) {
	case *agentv1.ExecServerMessage_ShellArgs:
		message.Type = wireShell
	case *agentv1.ExecServerMessage_ShellStreamArgs:
		message.Type = wireShellStream
	case *agentv1.ExecServerMessage_BackgroundShellSpawnArgs:
		message.Type = wireBackgroundShell
	case *agentv1.ExecServerMessage_WriteArgs:
		message.Type = wireWrite
	case *agentv1.ExecServerMessage_DeleteArgs:
		message.Type = wireDelete
	case *agentv1.ExecServerMessage_GrepArgs:
		message.Type = wireGrep
	case *agentv1.ExecServerMessage_ReadArgs, *agentv1.ExecServerMessage_RedactedReadArgs:
		message.Type = wireRead
	case *agentv1.ExecServerMessage_LsArgs:
		message.Type = wireList
	case *agentv1.ExecServerMessage_DiagnosticsArgs:
		message.Type = wireDiagnostics
	case *agentv1.ExecServerMessage_RequestContextArgs:
		message.Type = wireRequestContext
	case *agentv1.ExecServerMessage_McpArgs:
		message.Type = wireMCP
		decodeTypedMCPArgs(exec.GetMcpArgs(), message)
	case *agentv1.ExecServerMessage_FetchArgs:
		message.Type = wireFetch
	case *agentv1.ExecServerMessage_ShellAllowlistPrecheckArgs:
		message.Type = wireAllowlistPrecheck
		message.AllowlistKind = "shell"
	case *agentv1.ExecServerMessage_McpAllowlistPrecheckArgs:
		message.Type = wireAllowlistPrecheck
		message.AllowlistKind = "mcp"
	case *agentv1.ExecServerMessage_WebFetchAllowlistPrecheckArgs:
		message.Type = wireAllowlistPrecheck
		message.AllowlistKind = "web_fetch"
	case nil:
		unknown := unknownFieldNumbers(exec)
		if len(unknown) > 0 {
			return protocolDriftError("exec", unknown)
		}
		message.Type = wireUnsupportedExec
		message.Text = "empty exec payload"
	default:
		number := oneofFieldNumber(exec, exec.GetMessage())
		message.Type = wireUnsupportedExec
		message.Text = fmt.Sprintf("%T field %d", exec.GetMessage(), number)
	}
	return nil
}

func decodeTypedMCPArgs(args *agentv1.McpArgs, message *decodedWireMessage) {
	if args == nil {
		return
	}
	message.ToolCallID = strings.TrimSpace(args.GetToolCallId())
	message.ToolName = strings.TrimSpace(args.GetToolName())
	if message.ToolName == "" {
		message.ToolName = strings.TrimSpace(args.GetName())
	}
	arguments := make(map[string]any, len(args.GetArgs()))
	for key, raw := range args.GetArgs() {
		arguments[key] = decodeProtoValue(raw)
	}
	if encoded, err := jsonx.Marshal(arguments); err == nil {
		message.ToolArguments = string(encoded)
	}
}

func decodeTypedKVMessage(data []byte, message *decodedWireMessage) error {
	if err := rejectOverflowID(data, "KV"); err != nil {
		return err
	}
	kv := &agentv1.KvServerMessage{}
	if err := proto.Unmarshal(data, kv); err != nil {
		return fmt.Errorf("decode Cursor Agent v1 KV: %w", err)
	}
	message.ID = kv.GetId()
	switch typed := kv.GetMessage().(type) {
	case *agentv1.KvServerMessage_GetBlobArgs:
		message.Type = wireKVGet
		message.BlobID = append([]byte(nil), typed.GetBlobArgs.GetBlobId()...)
	case *agentv1.KvServerMessage_SetBlobArgs:
		message.Type = wireKVSet
		message.BlobID = append([]byte(nil), typed.SetBlobArgs.GetBlobId()...)
		message.BlobData = append([]byte(nil), typed.SetBlobArgs.GetBlobData()...)
	case nil:
		unknown := unknownFieldNumbers(kv)
		if len(unknown) > 0 {
			return protocolDriftError("KV", unknown)
		}
	default:
		return protocolDriftError("KV", []protowire.Number{oneofFieldNumber(kv, kv.GetMessage())})
	}
	return nil
}

func decodeTypedInteractionQuery(data []byte, message *decodedWireMessage) error {
	if err := rejectOverflowID(data, "interaction query"); err != nil {
		return err
	}
	query := &agentv1.InteractionQuery{}
	if err := proto.Unmarshal(data, query); err != nil {
		return fmt.Errorf("decode Cursor Agent v1 interaction query: %w", err)
	}
	message.Type = wireInteractionQuery
	message.ID = query.GetId()
	switch typed := query.GetQuery().(type) {
	case *agentv1.InteractionQuery_WebSearchRequestQuery:
		message.InteractionKind = 2
		if args := typed.WebSearchRequestQuery.GetArgs(); args != nil {
			message.ToolCallID = strings.TrimSpace(args.GetToolCallId())
		}
	case *agentv1.InteractionQuery_AskQuestionInteractionQuery:
		message.InteractionKind = 3
	case *agentv1.InteractionQuery_SwitchModeRequestQuery:
		message.InteractionKind = 4
	case *agentv1.InteractionQuery_ExaSearchRequestQuery:
		message.InteractionKind = 5
		if args := typed.ExaSearchRequestQuery.GetArgs(); args != nil {
			message.ToolCallID = strings.TrimSpace(args.GetToolCallId())
		}
	case *agentv1.InteractionQuery_ExaFetchRequestQuery:
		message.InteractionKind = 6
		if args := typed.ExaFetchRequestQuery.GetArgs(); args != nil {
			message.ToolCallID = strings.TrimSpace(args.GetToolCallId())
		}
	case *agentv1.InteractionQuery_CreatePlanRequestQuery:
		message.InteractionKind = 7
	case *agentv1.InteractionQuery_SetupVmEnvironmentArgs:
		message.InteractionKind = 8
	case *agentv1.InteractionQuery_WebFetchRequestQuery:
		message.InteractionKind = 9
		if args := typed.WebFetchRequestQuery.GetArgs(); args != nil {
			message.ToolCallID = strings.TrimSpace(args.GetToolCallId())
		}
	case nil:
		unknown := unknownFieldNumbers(query)
		if len(unknown) == 0 {
			return fmt.Errorf("unsupported Cursor Agent v1 interaction query kind 0")
		}
		return protocolDriftError("interaction query", unknown)
	default:
		return protocolDriftError("interaction query", []protowire.Number{oneofFieldNumber(query, query.GetQuery())})
	}
	return nil
}

func decodeTypedExecControl(data []byte, message *decodedWireMessage) error {
	control := &agentv1.ExecServerControlMessage{}
	if err := proto.Unmarshal(data, control); err != nil {
		return fmt.Errorf("decode Cursor Agent v1 exec control: %w", err)
	}
	if abort := control.GetAbort(); abort != nil {
		message.Type = wireExecAbort
		message.ID = abort.GetId()
		return nil
	}
	unknown := unknownFieldNumbers(control)
	if len(unknown) > 0 {
		return protocolDriftError("exec control", unknown)
	}
	return protocolDriftError("exec control", nil)
}

func encodeAllowlistPrecheck(id uint32, execID, kind string, allowlisted bool) ([]byte, error) {
	exec := &agentv1.ExecClientMessage{Id: id, ExecId: execID}
	switch kind {
	case "shell":
		exec.Message = &agentv1.ExecClientMessage_ShellAllowlistPrecheckResult{
			ShellAllowlistPrecheckResult: &agentv1.ShellAllowlistPrecheckResult{Allowlisted: allowlisted},
		}
	case "mcp":
		exec.Message = &agentv1.ExecClientMessage_McpAllowlistPrecheckResult{
			McpAllowlistPrecheckResult: &agentv1.McpAllowlistPrecheckResult{Allowlisted: allowlisted},
		}
	case "web_fetch":
		exec.Message = &agentv1.ExecClientMessage_WebFetchAllowlistPrecheckResult{
			WebFetchAllowlistPrecheckResult: &agentv1.WebFetchAllowlistPrecheckResult{Allowlisted: allowlisted},
		}
	default:
		return nil, fmt.Errorf("unsupported Cursor Agent v1 allowlist precheck %q", kind)
	}
	client := &agentv1.AgentClientMessage{
		Message: &agentv1.AgentClientMessage_ExecClientMessage{ExecClientMessage: exec},
	}
	return proto.Marshal(client)
}

func rejectOverflowID(data []byte, what string) error {
	remaining := data
	for len(remaining) > 0 {
		number, wireType, consumed := protowire.ConsumeTag(remaining)
		if consumed < 0 {
			return fmt.Errorf("decode Cursor Agent v1 %s tag: %v", what, protowire.ParseError(consumed))
		}
		remaining = remaining[consumed:]
		if wireType == protowire.VarintType && number == 1 {
			value, n := protowire.ConsumeVarint(remaining)
			if n < 0 {
				return fmt.Errorf("decode Cursor Agent v1 %s varint 1: %v", what, protowire.ParseError(n))
			}
			if value > uint64(^uint32(0)) {
				return fmt.Errorf("Cursor Agent v1 %s id exceeds uint32: %d", what, value)
			}
			remaining = remaining[n:]
			continue
		}
		n := protowire.ConsumeFieldValue(number, wireType, remaining)
		if n < 0 {
			return fmt.Errorf("decode Cursor Agent v1 %s field %d: %v", what, number, protowire.ParseError(n))
		}
		remaining = remaining[n:]
	}
	return nil
}

func unknownFieldNumbers(msg proto.Message) []protowire.Number {
	if msg == nil {
		return nil
	}
	unknown := msg.ProtoReflect().GetUnknown()
	var numbers []protowire.Number
	seen := map[protowire.Number]struct{}{}
	for len(unknown) > 0 {
		number, wireType, consumed := protowire.ConsumeTag(unknown)
		if consumed < 0 {
			return numbers
		}
		unknown = unknown[consumed:]
		n := protowire.ConsumeFieldValue(number, wireType, unknown)
		if n < 0 {
			return numbers
		}
		unknown = unknown[n:]
		if _, exists := seen[number]; exists {
			continue
		}
		seen[number] = struct{}{}
		numbers = append(numbers, number)
	}
	return numbers
}

func filterToolCallUnknown(numbers []protowire.Number) []protowire.Number {
	filtered := numbers[:0]
	for _, number := range numbers {
		switch number {
		case 54, 57, 59, 60:
			continue
		default:
			filtered = append(filtered, number)
		}
	}
	return filtered
}

func oneofFieldNumber(msg proto.Message, value any) protowire.Number {
	if msg == nil || value == nil {
		return 0
	}
	reflectMsg := msg.ProtoReflect()
	fields := reflectMsg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.ContainingOneof() == nil || !reflectMsg.Has(field) {
			continue
		}
		return protowire.Number(field.Number())
	}
	return 0
}
