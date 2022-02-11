package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"testing"
)

type testFileReader struct {
	content []byte
	err     error
}

func (tfr testFileReader) ReadFile(path string) ([]byte, error) {
	return tfr.content, tfr.err
}

type canConnectResponse struct {
	canConnect bool
	err        error
}
type testDatabase struct {
	connectResponse canConnectResponse
}

func (td testDatabase) CanConnect(ctx context.Context, ps PlanetScaleConnection) (bool, error) {
	return td.connectResponse.canConnect, td.connectResponse.err
}

func (td testDatabase) DiscoverSchema(ctx context.Context, ps PlanetScaleConnection) (Catalog, error) {
	//TODO implement me
	panic("implement me")
}

func (td testDatabase) Read(ctx context.Context, w io.Writer, ps PlanetScaleConnection, s Stream, state string) error {
	//TODO implement me
	panic("implement me")
}

func Test_Check_Invalid_Catalog_JSON(t *testing.T) {
	tfr := testFileReader{
		content: []byte("i am not json"),
	}
	checkCommand := CheckCommand(&Helper{
		Database:   PlanetScaleMySQLDatabase{},
		FileReader: tfr,
	})
	b := bytes.NewBufferString("")

	checkCommand.SetArgs([]string{"config source.json"})
	checkCommand.SetOut(b)
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
	})
	b := bytes.NewBufferString("")
	checkCommand.SetOut(b)
	checkCommand.Execute()
	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	var amsg AirbyteMessage
	err = json.Unmarshal(out, &amsg)
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
	})
	b := bytes.NewBufferString("")
	checkCommand.SetOut(b)
	checkCommand.Execute()
	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	var amsg AirbyteMessage
	err = json.Unmarshal(out, &amsg)
	assert.Equal(t, CONNECTION_STATUS, amsg.Type)
	assert.NotNil(t, amsg.ConnectionStatus)
	assert.Equal(t, "SUCCEEDED", amsg.ConnectionStatus.Status)
	successMsg := "Successfully connected to database database at host something.us-east-3.psdb.cloud with username username"
	assert.Equal(t, successMsg, amsg.ConnectionStatus.Message)
}
