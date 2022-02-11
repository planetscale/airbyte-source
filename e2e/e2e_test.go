package e2e

import (
	"bytes"
	"encoding/json"
	"github.com/planetscale/connect/source/cmd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestCheck(t *testing.T) {
	if _, ok := os.LookupEnv("END_TO_END_TEST_RUN"); !ok {
		t.Skip("Please run end-to-end tests with the script/e2e.sh script")
	}

	checkCommand := cmd.CheckCommand(cmd.DefaultHelper())
	sourceFileEnv, found := os.LookupEnv("SOURCE_CONFIG_FILE")
	require.True(t, found)
	checkCommand.Flag("config").Value.Set(sourceFileEnv)
	b := bytes.NewBufferString("")
	checkCommand.SetOut(b)
	checkCommand.Execute()
	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	var msg cmd.AirbyteMessage
	err = json.Unmarshal(out, &msg)
	assert.NoError(t, err)
	assert.Equal(t, cmd.CONNECTION_STATUS, msg.Type)
	require.NotNil(t, msg.ConnectionStatus)
	assert.Equal(t, "SUCCEEDED", msg.ConnectionStatus.Status)
}

func TestDiscover(t *testing.T) {
	if _, ok := os.LookupEnv("END_TO_END_TEST_RUN"); !ok {
		t.Skip("Please run end-to-end tests with the script/e2e.sh script")
	}
	discover := cmd.DiscoverCommand(cmd.DefaultHelper())
	sourceFileEnv, found := os.LookupEnv("SOURCE_CONFIG_FILE")
	require.True(t, found)
	discover.Flag("config").Value.Set(sourceFileEnv)
	b := bytes.NewBufferString("")
	discover.SetOut(b)
	discover.Execute()
	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	var msg cmd.AirbyteMessage
	err = json.Unmarshal(out, &msg)
	assert.NoError(t, err)
	assert.Equal(t, cmd.CATALOG, msg.Type)
	require.NotNil(t, msg.Catalog)
	s, err := json.Marshal(msg.Catalog)
	assert.NoError(t, err)
	fullCatalog, err := ioutil.ReadFile("../fixture/sakila-db/full_catalog.json")
	assert.NoError(t, err)
	assert.Equal(t, string(fullCatalog), string(s))
}
