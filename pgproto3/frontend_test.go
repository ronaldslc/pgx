package pgproto3

import (
	"testing"
)

func BenchmarkBackendMessageDecodeBySwitch(b *testing.B) {
	var authentication Authentication
	var backendKeyData BackendKeyData
	var bindComplete BindComplete
	var closeComplete CloseComplete
	var commandComplete CommandComplete
	var copyBothResponse CopyBothResponse
	var copyData CopyData
	var copyInResponse CopyInResponse
	var copyOutResponse CopyOutResponse
	var dataRow DataRow
	var emptyQueryResponse EmptyQueryResponse
	var errorResponse ErrorResponse
	var functionCallResponse FunctionCallResponse
	var noData NoData
	var noticeResponse NoticeResponse
	var notificationResponse NotificationResponse
	var parameterDescription ParameterDescription
	var parameterStatus ParameterStatus
	var parseComplete ParseComplete
	var readyForQuery ReadyForQuery
	var rowDescription RowDescription

	data := []byte{'1', '2', '3', 'A', 'C', 'd', 'D', 'E', 'G', 'H', 'I', 'K', 'n', 'N', 'R', 'S', 't', 'T', 'V', 'W', 'Z'}

	for i := 0; i < b.N; i++ {
		for _, d := range data {
			var msg BackendMessage
			switch d {
			case '1':
				msg = &parseComplete
			case '2':
				msg = &bindComplete
			case '3':
				msg = &closeComplete
			case 'A':
				msg = &notificationResponse
			case 'C':
				msg = &commandComplete
			case 'd':
				msg = &copyData
			case 'D':
				msg = &dataRow
			case 'E':
				msg = &errorResponse
			case 'G':
				msg = &copyInResponse
			case 'H':
				msg = &copyOutResponse
			case 'I':
				msg = &emptyQueryResponse
			case 'K':
				msg = &backendKeyData
			case 'n':
				msg = &noData
			case 'N':
				msg = &noticeResponse
			case 'R':
				msg = &authentication
			case 'S':
				msg = &parameterStatus
			case 't':
				msg = &parameterDescription
			case 'T':
				msg = &rowDescription
			case 'V':
				msg = &functionCallResponse
			case 'W':
				msg = &copyBothResponse
			case 'Z':
				msg = &readyForQuery
			default:
				b.Fatalf("unknown message type: %c", d)
			}
			_ = msg
		}
	}
}

func BenchmarkBackendMessageDecodeByArray(b *testing.B) {
	var backendMsgFlyweights [256]BackendMessage
	backendMsgFlyweights[uint8('1')] = &ParseComplete{}
	backendMsgFlyweights[uint8('2')] = &BindComplete{}
	backendMsgFlyweights[uint8('3')] = &CloseComplete{}
	backendMsgFlyweights[uint8('A')] = &NotificationResponse{}
	backendMsgFlyweights[uint8('C')] = &CommandComplete{}
	backendMsgFlyweights[uint8('d')] = &CopyData{}
	backendMsgFlyweights[uint8('D')] = &DataRow{}
	backendMsgFlyweights[uint8('E')] = &ErrorResponse{}
	backendMsgFlyweights[uint8('G')] = &CopyInResponse{}
	backendMsgFlyweights[uint8('H')] = &CopyOutResponse{}
	backendMsgFlyweights[uint8('I')] = &EmptyQueryResponse{}
	backendMsgFlyweights[uint8('K')] = &BackendKeyData{}
	backendMsgFlyweights[uint8('n')] = &NoData{}
	backendMsgFlyweights[uint8('N')] = &NoticeResponse{}
	backendMsgFlyweights[uint8('R')] = &Authentication{}
	backendMsgFlyweights[uint8('S')] = &ParameterStatus{}
	backendMsgFlyweights[uint8('t')] = &ParameterDescription{}
	backendMsgFlyweights[uint8('T')] = &RowDescription{}
	backendMsgFlyweights[uint8('V')] = &FunctionCallResponse{}
	backendMsgFlyweights[uint8('W')] = &CopyBothResponse{}
	backendMsgFlyweights[uint8('Z')] = &ReadyForQuery{}

	data := []byte{'1', '2', '3', 'A', 'C', 'd', 'D', 'E', 'G', 'H', 'I', 'K', 'n', 'N', 'R', 'S', 't', 'T', 'V', 'W', 'Z'}

	for i := 0; i < b.N; i++ {
		for _, d := range data {
			msg := backendMsgFlyweights[d]
			if msg == nil {
				b.Fatalf("unknown message type: %c", d)
			}
		}
	}
}
