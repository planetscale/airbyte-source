package airbyte_source

import (
	"fmt"
	"os"
	"testing"

	"github.com/planetscale/airbyte-source/cmd/internal"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/assert"
)

// This tests that when starting_gtids are passed AND a state file is passed,
// the state file takes precedence.
func TestRead_StartingGtidsAndState(t *testing.T) {
	psc := internal.PlanetScaleSource{
		Host:          "aws.connect.psdb.cloud",
		Database:      "sharded",
		Username:      "user",
		Password:      "pscale_password",
		StartingGtids: "{\"sharded\": {\"-80\": \"MySQL56/MyGTID:1-3\"}}",
	}

	numberType := "number"

	streams := []internal.ConfiguredStream{
		{
			Stream: internal.Stream{
				Name: "table1",
				Schema: internal.StreamSchema{
					Type: "object",
					Properties: map[string]internal.PropertyType{
						"id": {
							Type:        &numberType,
							AirbyteType: "integer",
						},
					},
				},
				SupportedSyncModes: []string{
					"full_refresh",
					"incremental",
				},
				Namespace: "sharded",
				PrimaryKeys: [][]string{
					{"id"},
				},
				SourceDefinedCursor: true,
				DefaultCursorFields: []string{
					"id",
				},
			},
			SyncMode: "incremental",
		},
		{
			Stream: internal.Stream{
				Name: "table2",
				Schema: internal.StreamSchema{
					Type: "object",
					Properties: map[string]internal.PropertyType{
						"id": {
							Type:        &numberType,
							AirbyteType: "integer",
						},
					},
				},
				SupportedSyncModes: []string{
					"full_refresh",
					"incremental",
				},
				Namespace: "sharded",
				PrimaryKeys: [][]string{
					{"id"},
				},
				SourceDefinedCursor: true,
				DefaultCursorFields: []string{
					"id",
				},
			},
			SyncMode: "incremental",
		},
	}
	shards := []string{"-80", "80-"}

	firstCursor, err := internal.TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:    "-80",
		Keyspace: "sharded",
		Position: "MySQL56/MyOtherGTID:1-3",
	})
	assert.NoError(t, err)
	secondCursor, err := internal.TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:    "80-",
		Keyspace: "sharded",
		Position: "MySQL56/MyOtherGTID:1-3",
	})
	assert.NoError(t, err)
	state := fmt.Sprintf("{\"streams\":{\"sharded:table1\":{\"shards\":{\"-80\":{\"cursor\":\"%s\"},\"80-\":{\"cursor\":\"%s\"}}},\"sharded:table2\":{\"shards\":{\"-80\":{\"cursor\":\"%s\"},\"80-\":{\"cursor\":\"%s\"}}}}}", firstCursor.Cursor, secondCursor.Cursor, firstCursor.Cursor, secondCursor.Cursor)

	expectedStates := internal.SyncState{
		Streams: map[string]internal.ShardStates{
			"sharded:table1": {
				Shards: map[string]*internal.SerializedCursor{
					"-80": firstCursor,
					"80-": secondCursor,
				},
			},
			"sharded:table2": {
				Shards: map[string]*internal.SerializedCursor{
					"-80": firstCursor,
					"80-": secondCursor,
				},
			},
		},
	}
	syncStates, err := readState(state, psc, streams, shards, internal.NewLogger(os.Stdout))
	assert.NoError(t, err)
	assert.Equal(t, expectedStates, syncStates)
}
