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

func Test_Check_Fails_Without_Config(t *testing.T) {
	checkCommand := CheckCommand(&Helper{
		Logger: NewLogger(),
	})
	b := bytes.NewBufferString("")
	checkCommand.SetOut(b)
	checkCommand.Execute()
	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	assert.Equal(t, "Please provide path to a valid configuration file\n", string(out))
}

func Test_Check_Invalid_Catalog_JSON(t *testing.T) {
	tfr := testFileReader{
		content: []byte("i am not json"),
	}
	checkCommand := CheckCommand(&Helper{
		Database:   PlanetScaleMySQLDatabase{},
		FileReader: tfr,
		Logger:     NewLogger(),
	})
	b := bytes.NewBufferString("")

	checkCommand.SetArgs([]string{"config source.json"})
	checkCommand.SetOut(b)
	checkCommand.Flag("config").Value.Set("catalog.json")
	checkCommand.Execute()
	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	var amsg AirbyteMessage
	err = json.Unmarshal(out, &amsg)
	assert.NoError(t, err)
	assert.Equal(t, CONNECTION_STATUS, amsg.Type)
	require.NotNil(t, amsg.ConnectionStatus)
	assert.Equal(t, "FAILED", amsg.ConnectionStatus.Status)
}

func Test_Check_Credentials_Invalid(t *testing.T) {
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
		Logger:     NewLogger(),
	})
	b := bytes.NewBufferString("")
	checkCommand.SetOut(b)
	checkCommand.Flag("config").Value.Set("catalog.json")
	checkCommand.Execute()
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

func Test_Check_Execute_Successful(t *testing.T) {
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
		Logger:     NewLogger(),
	})
	b := bytes.NewBufferString("")
	checkCommand.SetOut(b)

	checkCommand.Flag("config").Value.Set("catalog.json")
	checkCommand.Execute()
	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	var amsg AirbyteMessage
	err = json.Unmarshal(out, &amsg)
	require.NoError(t, err)
	assert.Equal(t, CONNECTION_STATUS, amsg.Type)
	assert.NotNil(t, amsg.ConnectionStatus)
	assert.Equal(t, "SUCCEEDED", amsg.ConnectionStatus.Status)
	successMsg := "Successfully connected to database database at host something.us-east-3.psdb.cloud with username username"
	assert.Equal(t, successMsg, amsg.ConnectionStatus.Message)
}
