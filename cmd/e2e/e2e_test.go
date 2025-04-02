package e2e

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	airbyte_source "github.com/planetscale/airbyte-source/cmd/airbyte-source"
	"github.com/planetscale/airbyte-source/cmd/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheck(t *testing.T) {
	if _, ok := os.LookupEnv("PS_END_TO_END_TEST_RUN"); !ok {
		t.Skip("Please run end-to-end tests with the script/e2e.sh script")
	}

	checkCommand := airbyte_source.CheckCommand(airbyte_source.DefaultHelper(os.Stdout))
	sourceFileEnv, found := os.LookupEnv("SOURCE_CONFIG_FILE")
	require.True(t, found)
	assert.NoError(t, checkCommand.Flag("config").Value.Set(sourceFileEnv))
	b := bytes.NewBufferString("")
	checkCommand.SetOut(b)
	assert.NoError(t, checkCommand.Execute())
	var msg internal.AirbyteMessage
	err := json.NewDecoder(b).Decode(&msg)
	assert.NoError(t, err)
	assert.Equal(t, internal.CONNECTION_STATUS, msg.Type)
	require.NotNil(t, msg.ConnectionStatus)
	assert.Equal(t, "SUCCEEDED", msg.ConnectionStatus.Status)
}

func TestDiscover(t *testing.T) {
	if _, ok := os.LookupEnv("PS_END_TO_END_TEST_RUN"); !ok {
		t.Skip("Please run end-to-end tests with the script/e2e.sh script")
	}
	discover := airbyte_source.DiscoverCommand(airbyte_source.DefaultHelper(os.Stdout))
	sourceFileEnv, found := os.LookupEnv("SOURCE_CONFIG_FILE")
	require.True(t, found)
	assert.NoError(t, discover.Flag("config").Value.Set(sourceFileEnv))
	b := bytes.NewBufferString("")
	discover.SetOut(b)
	assert.NoError(t, discover.Execute())
	var msg internal.AirbyteMessage
	err := json.NewDecoder(b).Decode(&msg)
	assert.NoError(t, err)
	assert.Equal(t, internal.CATALOG, msg.Type)
	require.NotNil(t, msg.Catalog)
	s, err := json.Marshal(msg.Catalog)
	assert.NoError(t, err)
	fullCatalog, err := os.ReadFile("../../fixture/sakila-db/full_catalog.json")
	assert.NoError(t, err)
	assert.Equal(t, string(fullCatalog), string(s))
}
