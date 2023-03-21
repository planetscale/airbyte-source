package airbyte_source

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/planetscale/airbyte-source/cmd/types"
	"github.com/planetscale/airbyte-source/shared"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckFailsWithoutConfig(t *testing.T) {
	checkCommand := CheckCommand(&Helper{
		Logger: types.NewLogger(os.Stdout),
	})
	b := bytes.NewBufferString("")
	checkCommand.SetOut(b)
	checkCommand.Execute()
	assert.Equal(t, "Please provide path to a valid configuration file\n", b.String())
}

func TestCheckInvalidCatalogJSON(t *testing.T) {
	tfr := testFileReader{
		content: []byte("i am not json"),
	}
	checkCommand := CheckCommand(&Helper{
		Database:   shared.PlanetScaleEdgeDatabase{},
		FileReader: tfr,
		Logger:     types.NewLogger(os.Stdout),
	})
	b := bytes.NewBufferString("")
	checkCommand.SetArgs([]string{"config source.json"})
	checkCommand.SetOut(b)
	checkCommand.Flag("config").Value.Set("catalog.json")
	checkCommand.Execute()

	var amsg types.AirbyteMessage
	err := json.NewDecoder(b).Decode(&amsg)
	assert.NoError(t, err)
	assert.Equal(t, types.CONNECTION_STATUS, amsg.Type)
	require.NotNil(t, amsg.ConnectionStatus)
	assert.Equal(t, "FAILED", amsg.ConnectionStatus.Status)
}

func TestCheckCredentialsInvalid(t *testing.T) {
	tfr := testFileReader{
		content: []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"),
	}

	td := testDatabase{
		connectResponse: canConnectResponse{
			err: fmt.Errorf("[%v] is invalid", "username"),
		},
	}

	checkCommand := CheckCommand(&Helper{
		Database:   td,
		FileReader: tfr,
		Logger:     types.NewLogger(os.Stdout),
	})
	b := bytes.NewBufferString("")
	checkCommand.SetOut(b)
	checkCommand.Flag("config").Value.Set("catalog.json")
	checkCommand.Execute()

	var amsg types.AirbyteMessage
	err := json.NewDecoder(b).Decode(&amsg)
	require.NoError(t, err)
	assert.Equal(t, types.CONNECTION_STATUS, amsg.Type)
	assert.NotNil(t, amsg.ConnectionStatus)
	assert.Equal(t, "FAILED", amsg.ConnectionStatus.Status)
	assert.Contains(t, amsg.ConnectionStatus.Message, "[username] is invalid")
}

func TestCheckExecuteSuccessful(t *testing.T) {
	tfr := testFileReader{
		content: []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"),
	}

	td := testDatabase{
		connectResponse: canConnectResponse{
			err: nil,
		},
	}

	checkCommand := CheckCommand(&Helper{
		Database:   td,
		FileReader: tfr,
		Logger:     types.NewLogger(os.Stdout),
	})
	b := bytes.NewBufferString("")
	checkCommand.SetOut(b)

	checkCommand.Flag("config").Value.Set("catalog.json")
	checkCommand.Execute()

	var amsg types.AirbyteMessage
	err := json.NewDecoder(b).Decode(&amsg)
	require.NoError(t, err)
	assert.Equal(t, types.CONNECTION_STATUS, amsg.Type)
	assert.NotNil(t, amsg.ConnectionStatus)
	assert.Equal(t, "SUCCEEDED", amsg.ConnectionStatus.Status)
	successMsg := "Successfully connected to database database at host something.us-east-3.psdb.cloud with username username"
	assert.Equal(t, successMsg, amsg.ConnectionStatus.Message)
}
