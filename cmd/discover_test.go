package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
)

func Test_Discover_Invalid_Source(t *testing.T) {
	tfr := testFileReader{
		content: []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"),
	}
	td := testDatabase{
		connectResponse: canConnectResponse{
			canConnect: false,
			err:        fmt.Errorf("[%v] is invalid", "username"),
		},
	}
	discover := DiscoverCommand(&Helper{
		Database:   td,
		FileReader: tfr,
		Logger:     NewLogger(),
	})
	discover.SetArgs([]string{"config source.json"})
	b := bytes.NewBufferString("")
	discover.SetOut(b)
	discover.Execute()
	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	var amsg AirbyteMessage
	err = json.Unmarshal(out, &amsg)
	require.NoError(t, err)
	assert.Equal(t, CONNECTION_STATUS, amsg.Type)
	assert.NotNil(t, amsg.ConnectionStatus)
	assert.Equal(t, "FAILED", amsg.ConnectionStatus.Status)
	assert.Contains(t, amsg.ConnectionStatus.Message, "[username] is invalid")
}

func Test_Discover_Failed(t *testing.T) {
	tfr := testFileReader{
		content: []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"),
	}
	td := testDatabase{
		connectResponse: canConnectResponse{
			canConnect: true,
		},
		discoverSchemaResponse: discoverSchemaResponse{
			err: fmt.Errorf("unable to get catalog for %v", "keyspace"),
		},
	}
	discover := DiscoverCommand(&Helper{
		Database:   td,
		FileReader: tfr,
		Logger:     NewLogger(),
	})
	discover.SetArgs([]string{"config source.json"})
	b := bytes.NewBufferString("")
	discover.SetOut(b)
	discover.Execute()
	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	var amsg AirbyteMessage
	err = json.Unmarshal(out, &amsg)
	require.NoError(t, err)
	assert.Equal(t, LOG, amsg.Type)
	require.NotNil(t, amsg.Log)
	assert.Equal(t, LOGLEVEL_ERROR, amsg.Log.Level)
	assert.Equal(t, "Unable to discover database, failed with [unable to get catalog for keyspace]", amsg.Log.Message)
}
