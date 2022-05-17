package airbyte_source

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/planetscale/connect/source/cmd/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"os/exec"
	"testing"
)

func TestRead_CanCheckConnectionBeforeRead(t *testing.T) {
	tfr := testFileReader{
		content: []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"),
	}

	td := testDatabase{
		connectResponse: canConnectResponse{
			canConnect: false,
			err:        fmt.Errorf("[%v] is invalid", "username"),
		},
	}

	readCommand := ReadCommand(&Helper{
		Database:   td,
		FileReader: tfr,
		Logger:     internal.NewLogger(os.Stdout),
	})

	b := bytes.NewBufferString("")
	readCommand.SetOut(b)
	readCommand.Flag("config").Value.Set("catalog.json")
	readCommand.Flag("catalog").Value.Set("catalog.json")
	fmt.Println("here")
	err := readCommand.Execute()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		fmt.Println("doing nothing")
		return
	}
	fmt.Printf("error is %v", err)

	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err, "should not fail to read output stream")
	var amsg internal.AirbyteMessage
	err = json.Unmarshal(out, &amsg)
	require.NoError(t, err, "should not fail to unmarshal airbyte message")
	assert.Equal(t, internal.CONNECTION_STATUS, amsg.Type)
	assert.NotNil(t, amsg.ConnectionStatus)
	assert.Equal(t, "FAILED", amsg.ConnectionStatus.Status)
	assert.Contains(t, amsg.ConnectionStatus.Message, "[username] is invalid")
}
