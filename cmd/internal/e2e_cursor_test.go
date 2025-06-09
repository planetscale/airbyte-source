package internal

import (
	"encoding/json"
	"testing"
	
	"github.com/stretchr/testify/assert"
)

func TestConfiguredStreamParsing(t *testing.T) {
	// Test that we can properly parse a configured catalog with cursor_field
	catalogJSON := `{
		"streams": [{
			"stream": {
				"name": "users",
				"namespace": "test_db",
				"json_schema": {},
				"supported_sync_modes": ["incremental"],
				"source_defined_cursor": false
			},
			"sync_mode": "incremental",
			"cursor_field": ["updated_at"]
		}]
	}`
	
	var catalog ConfiguredCatalog
	err := json.Unmarshal([]byte(catalogJSON), &catalog)
	assert.NoError(t, err)
	assert.Len(t, catalog.Streams, 1)
	assert.Equal(t, "incremental", catalog.Streams[0].SyncMode)
	assert.Equal(t, []string{"updated_at"}, catalog.Streams[0].CursorField)
}

func TestSerializedCursorWithUserDefinedValue(t *testing.T) {
	// Test that we can serialize/deserialize cursor with user-defined value
	original := &SerializedCursor{
		Cursor: "base64encodedgtid",
		UserDefinedCursorValue: "2024-01-15 10:30:00",
	}
	
	// Serialize to JSON
	data, err := json.Marshal(original)
	assert.NoError(t, err)
	
	// Deserialize back
	var decoded SerializedCursor
	err = json.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	
	assert.Equal(t, original.Cursor, decoded.Cursor)
	assert.Equal(t, original.UserDefinedCursorValue, decoded.UserDefinedCursorValue)
}

func TestStateWithUserDefinedCursor(t *testing.T) {
	// Test full state serialization with user-defined cursor
	state := AirbyteState{
		Data: SyncState{
			Streams: map[string]ShardStates{
				"test_db:users": {
					Shards: map[string]*SerializedCursor{
						"-": {
							Cursor: "gtidposition",
							UserDefinedCursorValue: "2024-01-15 10:30:00",
						},
					},
				},
			},
		},
	}
	
	// Serialize
	data, err := json.Marshal(state)
	assert.NoError(t, err)
	
	// Deserialize
	var decoded AirbyteState
	err = json.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	
	cursor := decoded.Data.Streams["test_db:users"].Shards["-"]
	assert.Equal(t, "gtidposition", cursor.Cursor)
	assert.Equal(t, "2024-01-15 10:30:00", cursor.UserDefinedCursorValue)
}