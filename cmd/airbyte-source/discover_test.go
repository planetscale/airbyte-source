package airbyte_source

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/planetscale/airbyte-source/cmd/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscoverInvalidSource(t *testing.T) {
	tfr := testFileReader{
		content: []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"),
	}
	td := testConnectClient{
		connectResponse: canConnectResponse{
			err: fmt.Errorf("[%v] is invalid", "username"),
		},
	}

	b := bytes.NewBufferString("")
	discover := DiscoverCommand(&Helper{
		ConnectClient: td,
		FileReader:    tfr,
		Logger:        internal.NewLogger(b),
	})
	discover.SetArgs([]string{"config source.json"})

	discover.SetOut(b)
	discover.Flag("config").Value.Set("catalog.json")
	discover.Execute()

	var amsg internal.AirbyteMessage
	err := json.NewDecoder(b).Decode(&amsg)
	require.NoError(t, err)
	assert.Equal(t, internal.CONNECTION_STATUS, amsg.Type)
	assert.NotNil(t, amsg.ConnectionStatus)
	assert.Equal(t, "FAILED", amsg.ConnectionStatus.Status)
	assert.Contains(t, amsg.ConnectionStatus.Message, "[username] is invalid")
}

func TestDiscoverFailed(t *testing.T) {
	tfr := testFileReader{
		content: []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"),
	}
	td := testConnectClient{
		connectResponse: canConnectResponse{
			err: nil,
		},
	}
	tmc := testMysqlClient{
		buildSchemaResponse: buildSchemaResponse{
			err: fmt.Errorf("unable to get catalog for %v", "keyspace"),
		},
	}
	b := bytes.NewBufferString("")
	discover := DiscoverCommand(&Helper{
		ConnectClient: td,
		MysqlClient:   tmc,
		FileReader:    tfr,
		Logger:        internal.NewLogger(b),
	})
	discover.SetArgs([]string{"config source.json"})

	discover.SetOut(b)
	discover.Flag("config").Value.Set("catalog.json")
	discover.Execute()
	var amsg internal.AirbyteMessage
	err := json.NewDecoder(b).Decode(&amsg)
	require.NoError(t, err)
	assert.Equal(t, internal.LOG, amsg.Type)
	require.NotNil(t, amsg.Log)
	assert.Equal(t, internal.LOGLEVEL_ERROR, amsg.Log.Level)
	assert.Equal(t, "PlanetScale Source :: Unable to discover database, failed with [unable to get catalog for keyspace]", amsg.Log.Message)
}
