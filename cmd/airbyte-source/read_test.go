package airbyte_source

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/planetscale/airbyte-source/lib"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"

	"vitess.io/vitess/go/sqltypes"

	"github.com/planetscale/airbyte-source/cmd/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadInvalidSource(t *testing.T) {
	tfr := testFileReader{
		Readfn: func(path string) ([]byte, error) {
			if path == "config.json" {
				return []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"), nil
			} else if path == "catalog.json" {
				return []byte("{\"streams\":[{\"stream\":{\"name\":\"Comment\",\"json_schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"postId\":{\"type\":\"integer\"},\"content\":{\"type\":\"string\"},\"authorId\":{\"type\":\"string\"},\"createdAt\":{\"type\":\"string\"},\"contentHtml\":{\"type\":\"string\"}}},\"supported_sync_modes\":[\"full_refresh\",\"incremental\"],\"source_defined_cursor\":true,\"default_cursor_field\":[\"id\"],\"source_defined_primary_key\":[[\"id\"]],\"namespace\":\"beam\"},\"sync_mode\":\"incremental\",\"cursor_field\":[\"id\"],\"destination_sync_mode\":\"append\",\"primary_key\":[[\"id\"]]}]}"), nil
			}
			return nil, fmt.Errorf("unknown path : %v", path)
		},
	}

	tcc := &lib.TestConnectClient{
		CanConnectFn: func(ctx context.Context, ps lib.PlanetScaleSource) error {
			return fmt.Errorf("[%v] is invalid", "username")
		},
	}

	b := bytes.NewBufferString("")
	read := ReadCommand(&Helper{
		Connect:    tcc,
		FileReader: tfr,
		Serializer: internal.NewSerializer(b),
	})
	read.SetArgs([]string{"config source.json"})

	read.SetOut(b)
	read.Flag("config").Value.Set("config.json")
	read.Flag("catalog").Value.Set("catalog.json")
	read.Execute()

	var amsg internal.AirbyteMessage

	err := json.NewDecoder(b).Decode(&amsg)
	require.NoError(t, err)

	assert.Equal(t, internal.CONNECTION_STATUS, amsg.Type)
	assert.NotNil(t, amsg.ConnectionStatus)
	assert.Equal(t, "FAILED", amsg.ConnectionStatus.Status)
	assert.Contains(t, amsg.ConnectionStatus.Message, "[username] is invalid")
}

func TestReadInvalidCatalog(t *testing.T) {
	tfr := testFileReader{
		Readfn: func(path string) ([]byte, error) {
			if path == "config.json" {
				return []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"), nil
			} else if path == "catalog.json" {
				return []byte("i-am-not-json"), nil
			}
			return nil, fmt.Errorf("unknown path : %v", path)
		},
	}

	tcc := &lib.TestConnectClient{
		CanConnectFn: func(ctx context.Context, ps lib.PlanetScaleSource) error {
			return nil
		},
	}

	b := bytes.NewBufferString("")
	read := ReadCommand(&Helper{
		Connect:    tcc,
		FileReader: tfr,
		Serializer: internal.NewSerializer(b),
	})
	read.SetArgs([]string{"config source.json"})

	read.SetOut(b)
	read.Flag("config").Value.Set("config.json")
	read.Flag("catalog").Value.Set("catalog.json")
	read.Execute()

	var amsg internal.AirbyteMessage
	err := json.NewDecoder(b).Decode(&amsg)
	require.NoError(t, err)

	assert.Equal(t, internal.LOG, amsg.Type)
	assert.NotNil(t, amsg.Log)
	assert.Equal(t, "Unable to read catalog", amsg.Log.Message)
}

func TestReadCanOutputRows(t *testing.T) {
	tfr := testFileReader{
		Readfn: func(path string) ([]byte, error) {
			if path == "config.json" {
				return []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"beam\",\"username\":\"username\",\"password\":\"password\"}"), nil
			} else if path == "catalog.json" {
				return []byte("{\"streams\":[{\"stream\":{\"name\":\"Comment\",\"json_schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"postId\":{\"type\":\"integer\"},\"content\":{\"type\":\"string\"},\"authorId\":{\"type\":\"string\"},\"createdAt\":{\"type\":\"string\"},\"contentHtml\":{\"type\":\"string\"}}},\"supported_sync_modes\":[\"full_refresh\",\"incremental\"],\"source_defined_cursor\":true,\"default_cursor_field\":[\"id\"],\"source_defined_primary_key\":[[\"id\"]],\"namespace\":\"beam\"},\"sync_mode\":\"incremental\",\"cursor_field\":[\"id\"],\"destination_sync_mode\":\"append\",\"primary_key\":[[\"id\"]]}]}"), nil
			}
			return nil, fmt.Errorf("unknown path : %v", path)
		},
	}

	tcc := &lib.TestConnectClient{
		CanConnectFn: func(ctx context.Context, ps lib.PlanetScaleSource) error {
			return nil
		},
		ListShardsFn: func(ctx context.Context, ps lib.PlanetScaleSource) ([]string, error) {
			return []string{"-"}, nil
		},
		ReadFn: func(ctx context.Context, logger lib.DatabaseLogger, ps lib.PlanetScaleSource, tableName string, tc *psdbconnect.TableCursor, onResult lib.OnResult, onCursor lib.OnCursor) (*lib.SerializedCursor, error) {
			if tableName == "Comment" {
				result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
					"pid|description",
					"int64|varbinary"),
					"1|keyboard",
					"2|monitor",
				)

				onResult(result)
			}
			return nil, nil
		},
	}

	tw := &testWriter{}
	read := ReadCommand(&Helper{
		Connect:    tcc,
		FileReader: tfr,
		Serializer: internal.NewSerializer(tw),
	})
	read.SetArgs([]string{"config source.json"})
	read.SetOut(tw)
	read.Flag("config").Value.Set("config.json")
	read.Flag("catalog").Value.Set("catalog.json")
	read.Execute()

	var amsg internal.AirbyteMessage
	stateLine := tw.lines[0]
	err := json.Unmarshal([]byte(stateLine), &amsg)
	require.NoError(t, err)

	firstRecord := tw.lines[1]
	err = json.Unmarshal([]byte(firstRecord), &amsg)
	require.NoError(t, err)
	assert.Equal(t, internal.RECORD, amsg.Type)

	secondRecord := tw.lines[2]
	err = json.Unmarshal([]byte(secondRecord), &amsg)
	require.NoError(t, err)
	assert.Equal(t, internal.RECORD, amsg.Type)
}
