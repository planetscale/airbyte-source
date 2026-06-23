package internal

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamState_EmitsPerStreamFormat(t *testing.T) {
	b := bytes.NewBufferString("")
	logger := NewLogger(b)

	shardStates := ShardStates{
		Shards: map[string]*SerializedCursor{
			"-": {Cursor: "abc123"},
		},
	}

	logger.StreamState("my-database", "users", shardStates)

	var msg AirbyteMessage
	err := json.NewDecoder(b).Decode(&msg)
	require.NoError(t, err)

	assert.Equal(t, STATE, msg.Type)
	require.NotNil(t, msg.State)
	assert.Equal(t, STATE_TYPE_STREAM, msg.State.Type)
	require.NotNil(t, msg.State.Stream)
	assert.Equal(t, "users", msg.State.Stream.StreamDescriptor.Name)
	assert.Equal(t, "my-database", msg.State.Stream.StreamDescriptor.Namespace)
	require.NotNil(t, msg.State.Stream.StreamState)
	assert.Equal(t, "abc123", msg.State.Stream.StreamState.Shards["-"].Cursor)
}

func TestStreamState_MultipleShards(t *testing.T) {
	b := bytes.NewBufferString("")
	logger := NewLogger(b)

	shardStates := ShardStates{
		Shards: map[string]*SerializedCursor{
			"-80": {Cursor: "cursor1"},
			"80-": {Cursor: "cursor2"},
		},
	}

	logger.StreamState("sharded-db", "orders", shardStates)

	var msg AirbyteMessage
	err := json.NewDecoder(b).Decode(&msg)
	require.NoError(t, err)

	assert.Equal(t, STATE_TYPE_STREAM, msg.State.Type)
	assert.Equal(t, "orders", msg.State.Stream.StreamDescriptor.Name)
	assert.Equal(t, "sharded-db", msg.State.Stream.StreamDescriptor.Namespace)
	assert.Len(t, msg.State.Stream.StreamState.Shards, 2)
	assert.Equal(t, "cursor1", msg.State.Stream.StreamState.Shards["-80"].Cursor)
	assert.Equal(t, "cursor2", msg.State.Stream.StreamState.Shards["80-"].Cursor)
}

func TestStreamState_NoLegacyDataField(t *testing.T) {
	b := bytes.NewBufferString("")
	logger := NewLogger(b)

	shardStates := ShardStates{
		Shards: map[string]*SerializedCursor{
			"-": {Cursor: "abc"},
		},
	}

	logger.StreamState("db", "table1", shardStates)

	// Parse as raw JSON to verify no "data" key exists (which would indicate LEGACY format)
	var raw map[string]json.RawMessage
	err := json.NewDecoder(b).Decode(&raw)
	require.NoError(t, err)

	var stateRaw map[string]json.RawMessage
	err = json.Unmarshal(raw["state"], &stateRaw)
	require.NoError(t, err)

	_, hasData := stateRaw["data"]
	assert.False(t, hasData, "state should not contain 'data' field (LEGACY format)")

	_, hasType := stateRaw["type"]
	assert.True(t, hasType, "state must contain 'type' field")

	_, hasStream := stateRaw["stream"]
	assert.True(t, hasStream, "state must contain 'stream' field")
}

func TestStreamStatus_EmitsTraceMessage(t *testing.T) {
	b := bytes.NewBufferString("")
	logger := NewLogger(b)

	logger.StreamStatus("my-db", "accounts", STREAM_STATUS_STARTED)

	var msg AirbyteMessage
	err := json.NewDecoder(b).Decode(&msg)
	require.NoError(t, err)

	assert.Equal(t, TRACE, msg.Type)
	require.NotNil(t, msg.Trace)
	assert.Equal(t, TRACE_TYPE_STREAM_STATUS, msg.Trace.Type)
	assert.True(t, msg.Trace.EmittedAt > 0)
	require.NotNil(t, msg.Trace.StreamStatus)
	assert.Equal(t, "accounts", msg.Trace.StreamStatus.StreamDescriptor.Name)
	assert.Equal(t, "my-db", msg.Trace.StreamStatus.StreamDescriptor.Namespace)
	assert.Equal(t, STREAM_STATUS_STARTED, msg.Trace.StreamStatus.Status)
}

func TestStreamStatus_Complete(t *testing.T) {
	b := bytes.NewBufferString("")
	logger := NewLogger(b)

	logger.StreamStatus("ns", "tbl", STREAM_STATUS_COMPLETE)

	var msg AirbyteMessage
	err := json.NewDecoder(b).Decode(&msg)
	require.NoError(t, err)

	assert.Equal(t, STREAM_STATUS_COMPLETE, msg.Trace.StreamStatus.Status)
}

func TestStreamStatus_Incomplete(t *testing.T) {
	b := bytes.NewBufferString("")
	logger := NewLogger(b)

	logger.StreamStatus("ns", "tbl", STREAM_STATUS_INCOMPLETE)

	var msg AirbyteMessage
	err := json.NewDecoder(b).Decode(&msg)
	require.NoError(t, err)

	assert.Equal(t, STREAM_STATUS_INCOMPLETE, msg.Trace.StreamStatus.Status)
}

func TestStreamState_JSONRoundTrip(t *testing.T) {
	// Verify the JSON output can be parsed back into the exact expected Airbyte protocol structure
	b := bytes.NewBufferString("")
	logger := NewLogger(b)

	logger.StreamState("anam-lab", "persona", ShardStates{
		Shards: map[string]*SerializedCursor{
			"-": {Cursor: "encoded-cursor-data"},
		},
	})

	// Parse into a generic structure to verify exact JSON shape
	var raw map[string]interface{}
	err := json.NewDecoder(b).Decode(&raw)
	require.NoError(t, err)

	assert.Equal(t, "STATE", raw["type"])

	state := raw["state"].(map[string]interface{})
	assert.Equal(t, "STREAM", state["type"])

	stream := state["stream"].(map[string]interface{})
	descriptor := stream["stream_descriptor"].(map[string]interface{})
	assert.Equal(t, "persona", descriptor["name"])
	assert.Equal(t, "anam-lab", descriptor["namespace"])

	streamState := stream["stream_state"].(map[string]interface{})
	shards := streamState["shards"].(map[string]interface{})
	shard := shards["-"].(map[string]interface{})
	assert.Equal(t, "encoded-cursor-data", shard["cursor"])
}

func TestMultipleStreamStates_EachIndependent(t *testing.T) {
	b := bytes.NewBufferString("")
	logger := NewLogger(b)

	logger.StreamState("db", "table1", ShardStates{
		Shards: map[string]*SerializedCursor{"-": {Cursor: "c1"}},
	})
	logger.StreamState("db", "table2", ShardStates{
		Shards: map[string]*SerializedCursor{"-": {Cursor: "c2"}},
	})

	decoder := json.NewDecoder(b)

	var msg1 AirbyteMessage
	require.NoError(t, decoder.Decode(&msg1))
	assert.Equal(t, "table1", msg1.State.Stream.StreamDescriptor.Name)
	assert.Equal(t, "c1", msg1.State.Stream.StreamState.Shards["-"].Cursor)

	var msg2 AirbyteMessage
	require.NoError(t, decoder.Decode(&msg2))
	assert.Equal(t, "table2", msg2.State.Stream.StreamDescriptor.Name)
	assert.Equal(t, "c2", msg2.State.Stream.StreamState.Shards["-"].Cursor)
}
