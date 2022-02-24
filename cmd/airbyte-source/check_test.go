package airbyte_source

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/planetscale/connect/source/cmd/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
)

func TestCheckFailsWithoutConfig(t *testing.T) {
	checkCommand := CheckCommand(&Helper{
		Logger: internal.NewLogger(),
	})
	b := bytes.NewBufferString("")
	checkCommand.SetOut(b)
	checkCommand.Execute()
	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	assert.Equal(t, "Please provide path to a valid configuration file\n", string(out))
}

func TestCheckInvalidCatalogJSON(t *testing.T) {
	tfr := testFileReader{
		content: []byte("i am not json"),
	}
	checkCommand := CheckCommand(&Helper{
		Database:   internal.PlanetScaleMySQLDatabase{},
		FileReader: tfr,
		Logger:     internal.NewLogger(),
	})
	b := bytes.NewBufferString("")

	checkCommand.SetArgs([]string{"config source.json"})
	checkCommand.SetOut(b)
	checkCommand.Flag("config").Value.Set("catalog.json")
	checkCommand.Execute()
	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	var amsg internal.AirbyteMessage
	err = json.Unmarshal(out, &amsg)
	assert.NoError(t, err)
	assert.Equal(t, internal.CONNECTION_STATUS, amsg.Type)
	require.NotNil(t, amsg.ConnectionStatus)
	assert.Equal(t, "FAILED", amsg.ConnectionStatus.Status)
}

func TestCheckCredentialsInvalid(t *testing.T) {
	tfr := testFileReader{
		content: []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"),
	}

	td := testDatabase{
		connectResponse: canConnectResponse{
			canConnect: false,
			err:        fmt.Errorf("[%v] is invalid", "username"),
		},
	}

	checkCommand := CheckCommand(&Helper{
		Database:   td,
		FileReader: tfr,
		Logger:     internal.NewLogger(),
	})
	b := bytes.NewBufferString("")
	checkCommand.SetOut(b)
	checkCommand.Flag("config").Value.Set("catalog.json")
	checkCommand.Execute()
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

func TestCheckExecuteSuccessful(t *testing.T) {
	tfr := testFileReader{
		content: []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"),
	}

	td := testDatabase{
		connectResponse: canConnectResponse{
			canConnect: true,
		},
	}

	checkCommand := CheckCommand(&Helper{
		Database:   td,
		FileReader: tfr,
		Logger:     internal.NewLogger(),
	})
	b := bytes.NewBufferString("")
	checkCommand.SetOut(b)

	checkCommand.Flag("config").Value.Set("catalog.json")
	checkCommand.Execute()
	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	var amsg internal.AirbyteMessage
	err = json.Unmarshal(out, &amsg)
	require.NoError(t, err)
	assert.Equal(t, internal.CONNECTION_STATUS, amsg.Type)
	assert.NotNil(t, amsg.ConnectionStatus)
	assert.Equal(t, "SUCCEEDED", amsg.ConnectionStatus.Status)
	successMsg := "Successfully connected to database database at host something.us-east-3.psdb.cloud with username username"
	assert.Equal(t, successMsg, amsg.ConnectionStatus.Message)
}
