package airbyte_source

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/planetscale/airbyte-source/cmd/internal"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockDatabase implements internal.PlanetScaleDatabase for read protocol tests.
type mockDatabase struct {
	shards    []string
	readFunc  func(ctx context.Context, w io.Writer, ps internal.PlanetScaleSource, s internal.ConfiguredStream, tc *psdbconnect.TableCursor) (*internal.SerializedCursor, error)
	readCalls int
}

func (m *mockDatabase) CanConnect(ctx context.Context, ps internal.PlanetScaleSource) error {
	return nil
}

func (m *mockDatabase) DiscoverSchema(ctx context.Context, ps internal.PlanetScaleSource) (internal.Catalog, error) {
	return internal.Catalog{}, nil
}

func (m *mockDatabase) ListShards(ctx context.Context, ps internal.PlanetScaleSource) ([]string, error) {
	return m.shards, nil
}

func (m *mockDatabase) Read(ctx context.Context, w io.Writer, ps internal.PlanetScaleSource, s internal.ConfiguredStream, tc *psdbconnect.TableCursor) (*internal.SerializedCursor, error) {
	m.readCalls++
	if m.readFunc != nil {
		return m.readFunc(ctx, w, ps, s, tc)
	}
	newCursor, _ := internal.TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:    tc.Shard,
		Keyspace: tc.Keyspace,
		Position: "MySQL56/updated-position",
	})
	return newCursor, nil
}

func (m *mockDatabase) Close() error {
	return nil
}

func newTestConfig() []byte {
	return []byte(`{"host":"test.psdb.cloud","database":"testdb","username":"user","password":"pass"}`)
}

func newTestCatalog(t *testing.T, streams ...string) string {
	t.Helper()
	catalog := internal.ConfiguredCatalog{}
	for _, name := range streams {
		catalog.Streams = append(catalog.Streams, internal.ConfiguredStream{
			Stream: internal.Stream{
				Name:      name,
				Namespace: "testdb",
			},
			SyncMode: "full_refresh",
		})
	}
	b, err := json.Marshal(catalog)
	require.NoError(t, err)
	return string(b)
}

func writeTempFile(t *testing.T, content []byte) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.json")
	require.NoError(t, err)
	_, err = f.Write(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func parseOutputMessages(t *testing.T, buf *bytes.Buffer) []internal.AirbyteMessage {
	t.Helper()
	var messages []internal.AirbyteMessage
	decoder := json.NewDecoder(buf)
	for decoder.More() {
		var msg internal.AirbyteMessage
		if err := decoder.Decode(&msg); err != nil {
			break
		}
		messages = append(messages, msg)
	}
	return messages
}

func setupReadCommand(t *testing.T, db *mockDatabase, catalogJSON string) (*bytes.Buffer, *Helper) {
	t.Helper()
	b := bytes.NewBufferString("")
	h := &Helper{
		Database:   db,
		FileReader: testFileReader{content: newTestConfig()},
		Logger:     internal.NewLogger(b),
	}
	return b, h
}

func TestRead_EmitsPerStreamStateNotLegacy(t *testing.T) {
	db := &mockDatabase{shards: []string{"-"}}
	catalogJSON := newTestCatalog(t, "users")

	configFile := writeTempFile(t, newTestConfig())
	catalogFile := writeTempFile(t, []byte(catalogJSON))

	b, h := setupReadCommand(t, db, catalogJSON)
	cmd := ReadCommand(h)
	cmd.SetOut(b)
	require.NoError(t, cmd.Flag("config").Value.Set(configFile))
	require.NoError(t, cmd.Flag("catalog").Value.Set(catalogFile))
	require.NoError(t, cmd.Execute())

	messages := parseOutputMessages(t, b)

	var stateMessages []internal.AirbyteMessage
	for _, msg := range messages {
		if msg.Type == internal.STATE {
			stateMessages = append(stateMessages, msg)
		}
	}

	require.NotEmpty(t, stateMessages, "should emit at least one STATE message")

	for _, msg := range stateMessages {
		assert.Equal(t, internal.STATE_TYPE_STREAM, msg.State.Type,
			"state.type must be STREAM, not LEGACY")
		require.NotNil(t, msg.State.Stream,
			"state.stream must be present")
		assert.NotEmpty(t, msg.State.Stream.StreamDescriptor.Name,
			"stream_descriptor.name must be set")
		assert.NotEmpty(t, msg.State.Stream.StreamDescriptor.Namespace,
			"stream_descriptor.namespace must be set")
		require.NotNil(t, msg.State.Stream.StreamState,
			"stream_state must be present")
	}
}

func TestRead_EmitsStartedAndCompletePerStream(t *testing.T) {
	db := &mockDatabase{shards: []string{"-"}}
	catalogJSON := newTestCatalog(t, "orders", "products")

	configFile := writeTempFile(t, newTestConfig())
	catalogFile := writeTempFile(t, []byte(catalogJSON))

	b, h := setupReadCommand(t, db, catalogJSON)
	cmd := ReadCommand(h)
	cmd.SetOut(b)
	require.NoError(t, cmd.Flag("config").Value.Set(configFile))
	require.NoError(t, cmd.Flag("catalog").Value.Set(catalogFile))
	require.NoError(t, cmd.Execute())

	messages := parseOutputMessages(t, b)

	type streamStatusEntry struct {
		Name   string
		Status string
	}
	var statuses []streamStatusEntry
	for _, msg := range messages {
		if msg.Type == internal.TRACE && msg.Trace != nil &&
			msg.Trace.Type == internal.TRACE_TYPE_STREAM_STATUS &&
			msg.Trace.StreamStatus != nil {
			statuses = append(statuses, streamStatusEntry{
				Name:   msg.Trace.StreamStatus.StreamDescriptor.Name,
				Status: msg.Trace.StreamStatus.Status,
			})
		}
	}

	expectedStatuses := []streamStatusEntry{
		{"orders", internal.STREAM_STATUS_STARTED},
		{"orders", internal.STREAM_STATUS_COMPLETE},
		{"products", internal.STREAM_STATUS_STARTED},
		{"products", internal.STREAM_STATUS_COMPLETE},
	}
	assert.Equal(t, expectedStatuses, statuses)
}

func TestRead_StatePerStreamContainsCorrectDescriptor(t *testing.T) {
	db := &mockDatabase{shards: []string{"-"}}
	catalogJSON := newTestCatalog(t, "accounts", "sessions")

	configFile := writeTempFile(t, newTestConfig())
	catalogFile := writeTempFile(t, []byte(catalogJSON))

	b, h := setupReadCommand(t, db, catalogJSON)
	cmd := ReadCommand(h)
	cmd.SetOut(b)
	require.NoError(t, cmd.Flag("config").Value.Set(configFile))
	require.NoError(t, cmd.Flag("catalog").Value.Set(catalogFile))
	require.NoError(t, cmd.Execute())

	messages := parseOutputMessages(t, b)

	statesByStream := map[string]internal.AirbyteMessage{}
	for _, msg := range messages {
		if msg.Type == internal.STATE {
			name := msg.State.Stream.StreamDescriptor.Name
			statesByStream[name] = msg
		}
	}

	assert.Contains(t, statesByStream, "accounts")
	assert.Contains(t, statesByStream, "sessions")
	assert.Equal(t, "testdb", statesByStream["accounts"].State.Stream.StreamDescriptor.Namespace)
	assert.Equal(t, "testdb", statesByStream["sessions"].State.Stream.StreamDescriptor.Namespace)
}

func TestRead_StateEmittedAfterStartedBeforeComplete(t *testing.T) {
	db := &mockDatabase{shards: []string{"-"}}
	catalogJSON := newTestCatalog(t, "events")

	configFile := writeTempFile(t, newTestConfig())
	catalogFile := writeTempFile(t, []byte(catalogJSON))

	b, h := setupReadCommand(t, db, catalogJSON)
	cmd := ReadCommand(h)
	cmd.SetOut(b)
	require.NoError(t, cmd.Flag("config").Value.Set(configFile))
	require.NoError(t, cmd.Flag("catalog").Value.Set(catalogFile))
	require.NoError(t, cmd.Execute())

	messages := parseOutputMessages(t, b)

	startedIdx := -1
	stateIdx := -1
	completeIdx := -1

	for i, msg := range messages {
		if msg.Type == internal.TRACE && msg.Trace != nil &&
			msg.Trace.StreamStatus != nil &&
			msg.Trace.StreamStatus.StreamDescriptor.Name == "events" {
			if msg.Trace.StreamStatus.Status == internal.STREAM_STATUS_STARTED {
				startedIdx = i
			}
			if msg.Trace.StreamStatus.Status == internal.STREAM_STATUS_COMPLETE {
				completeIdx = i
			}
		}
		if msg.Type == internal.STATE && msg.State != nil &&
			msg.State.Stream != nil &&
			msg.State.Stream.StreamDescriptor.Name == "events" {
			stateIdx = i
		}
	}

	require.Greater(t, startedIdx, -1, "STARTED should be emitted")
	require.Greater(t, stateIdx, -1, "STATE should be emitted")
	require.Greater(t, completeIdx, -1, "COMPLETE should be emitted")

	assert.Less(t, startedIdx, stateIdx, "STARTED should come before STATE")
	assert.Less(t, stateIdx, completeIdx, "STATE should come before COMPLETE")
}

func TestRead_MultiShardStateContainsAllShards(t *testing.T) {
	db := &mockDatabase{shards: []string{"-80", "80-"}}
	catalogJSON := newTestCatalog(t, "data")

	configFile := writeTempFile(t, newTestConfig())
	catalogFile := writeTempFile(t, []byte(catalogJSON))

	b, h := setupReadCommand(t, db, catalogJSON)
	cmd := ReadCommand(h)
	cmd.SetOut(b)
	require.NoError(t, cmd.Flag("config").Value.Set(configFile))
	require.NoError(t, cmd.Flag("catalog").Value.Set(catalogFile))
	require.NoError(t, cmd.Execute())

	messages := parseOutputMessages(t, b)

	var stateMsg *internal.AirbyteMessage
	for _, msg := range messages {
		if msg.Type == internal.STATE {
			stateMsg = &msg
		}
	}

	require.NotNil(t, stateMsg, "should have a STATE message")
	require.NotNil(t, stateMsg.State.Stream.StreamState)
	assert.Len(t, stateMsg.State.Stream.StreamState.Shards, 2,
		"state should contain both shards")
	assert.Contains(t, stateMsg.State.Stream.StreamState.Shards, "-80")
	assert.Contains(t, stateMsg.State.Stream.StreamState.Shards, "80-")
}

func TestRead_ReadErrorEmitsIncompleteNotComplete(t *testing.T) {
	db := &mockDatabase{
		shards: []string{"-"},
		readFunc: func(ctx context.Context, w io.Writer, ps internal.PlanetScaleSource, s internal.ConfiguredStream, tc *psdbconnect.TableCursor) (*internal.SerializedCursor, error) {
			if s.Stream.Name == "bad_table" {
				return nil, assert.AnError
			}
			newCursor, _ := internal.TableCursorToSerializedCursor(&psdbconnect.TableCursor{
				Shard:    tc.Shard,
				Keyspace: tc.Keyspace,
				Position: "MySQL56/pos",
			})
			return newCursor, nil
		},
	}

	catalog := internal.ConfiguredCatalog{
		Streams: []internal.ConfiguredStream{
			{
				Stream:   internal.Stream{Name: "good_table", Namespace: "testdb"},
				SyncMode: "full_refresh",
			},
			{
				Stream:   internal.Stream{Name: "bad_table", Namespace: "testdb"},
				SyncMode: "full_refresh",
			},
		},
	}
	catalogBytes, _ := json.Marshal(catalog)

	configFile := writeTempFile(t, newTestConfig())
	catalogFile := writeTempFile(t, catalogBytes)

	b := bytes.NewBufferString("")
	h := &Helper{
		Database:   db,
		FileReader: testFileReader{content: newTestConfig()},
		Logger:     internal.NewLogger(b),
	}

	cmd := ReadCommand(h)
	cmd.SetOut(b)
	require.NoError(t, cmd.Flag("config").Value.Set(configFile))
	require.NoError(t, cmd.Flag("catalog").Value.Set(catalogFile))
	require.NoError(t, cmd.Execute())

	messages := parseOutputMessages(t, b)

	streamStatuses := map[string][]string{}
	for _, msg := range messages {
		if msg.Type == internal.TRACE && msg.Trace != nil &&
			msg.Trace.StreamStatus != nil {
			name := msg.Trace.StreamStatus.StreamDescriptor.Name
			streamStatuses[name] = append(streamStatuses[name], msg.Trace.StreamStatus.Status)
		}
	}

	assert.Equal(t, []string{internal.STREAM_STATUS_STARTED, internal.STREAM_STATUS_COMPLETE},
		streamStatuses["good_table"])
	assert.Equal(t, []string{internal.STREAM_STATUS_STARTED, internal.STREAM_STATUS_INCOMPLETE},
		streamStatuses["bad_table"])

	// good_table should have a STATE message, bad_table should NOT
	hasGoodState := false
	hasBadState := false
	for _, msg := range messages {
		if msg.Type == internal.STATE && msg.State != nil && msg.State.Stream != nil {
			if msg.State.Stream.StreamDescriptor.Name == "good_table" {
				hasGoodState = true
			}
			if msg.State.Stream.StreamDescriptor.Name == "bad_table" {
				hasBadState = true
			}
		}
	}
	assert.True(t, hasGoodState, "good_table should have a STATE message")
	assert.False(t, hasBadState, "bad_table should NOT have a STATE message (it failed)")
}
