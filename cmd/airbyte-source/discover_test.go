package airbyte_source

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/planetscale/airbyte-source/cmd/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscoverInvalidSource(t *testing.T) {
	tfr := testFileReader{
		content: []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"),
	}
	td := testDatabase{
		connectResponse: canConnectResponse{
			err: fmt.Errorf("[%v] is invalid", "username"),
		},
	}

	b := bytes.NewBufferString("")
	discover := DiscoverCommand(&Helper{
		Database:   td,
		FileReader: tfr,
		Logger:     internal.NewLogger(b),
	})
	discover.SetArgs([]string{"config source.json"})

	discover.SetOut(b)
	discover.Flag("config").Value.Set("catalog.json")
	discover.Execute()
	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	var amsg internal.AirbyteMessage
	err = json.Unmarshal(out, &amsg)
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
	td := testDatabase{
		connectResponse: canConnectResponse{
			err: nil,
		},
		discoverSchemaResponse: discoverSchemaResponse{
			err: fmt.Errorf("unable to get catalog for %v", "keyspace"),
		},
	}
	b := bytes.NewBufferString("")
	discover := DiscoverCommand(&Helper{
		Database:   td,
		FileReader: tfr,
		Logger:     internal.NewLogger(b),
	})
	discover.SetArgs([]string{"config source.json"})

	discover.SetOut(b)
	discover.Flag("config").Value.Set("catalog.json")
	discover.Execute()
	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	var amsg internal.AirbyteMessage
	err = json.Unmarshal(out, &amsg)
	require.NoError(t, err)
	assert.Equal(t, internal.LOG, amsg.Type)
	require.NotNil(t, amsg.Log)
	assert.Equal(t, internal.LOGLEVEL_ERROR, amsg.Log.Level)
	assert.Equal(t, "PlanetScale Source :: Unable to discover database, failed with [unable to get catalog for keyspace]", amsg.Log.Message)
}
