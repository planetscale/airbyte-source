package internal

import (
	"fmt"
	"testing"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/assert"
)

func TestCanGenerateSecureDSN(t *testing.T) {
	psc := PlanetScaleSource{
		Host:     "useast.psdb.connect",
		Username: "usernameus-east-4",
		Password: "pscale_password",
		Database: "connect-test",
	}
	dsn := psc.DSN()
	assert.Equal(t, "usernameus-east-4:pscale_password@tcp(useast.psdb.connect)/connect-test@primary?tls=true", dsn)
}

func TestCanGenerateInsecureDSN(t *testing.T) {
	psc := PlanetScaleSource{
		Host:     "useast.psdb.connect",
		Username: "usernameus-east-4",
		Password: "pscale_password",
		Database: "connect-test",
	}
	t.Setenv("PS_END_TO_END_TEST_RUN", "true")
	dsn := psc.DSN()
	assert.Equal(t, "usernameus-east-4:pscale_password@tcp(useast.psdb.connect)/connect-test?tls=skip-verify", dsn)
}

func TestCanGenerateInitialState_Sharded(t *testing.T) {
	psc := PlanetScaleSource{
		Host:     "useast.psdb.connect",
		Username: "usernameus-east-4",
		Password: "pscale_password",
		Database: "connect-test",
	}
	shards := []string{
		"-40",
		"40-80",
		"80-c0",
		"c0-",
	}
	shardStates, err := psc.GetInitialState("connect-test", shards)
	assert.NoError(t, err)
	expectedShardStates := ShardStates{
		Shards: map[string]*SerializedCursor{},
	}

	for _, shard := range shards {
		cursor, err := TableCursorToSerializedCursor(&psdbconnect.TableCursor{
			Shard:    shard,
			Keyspace: "connect-test",
			Position: "",
		})
		assert.NoError(t, err)
		expectedShardStates.Shards[shard] = cursor
	}

	assert.NoError(t, err)
	assert.Equal(t, expectedShardStates, shardStates)
}

func TestCanGenerateInitialState_CustomShards(t *testing.T) {
	psc := PlanetScaleSource{
		Host:     "useast.psdb.connect",
		Username: "usernameus-east-4",
		Password: "pscale_password",
		Database: "connect-test",
		Shards:   "80-c0",
	}
	shards := []string{
		"-40",
		"40-80",
		"80-c0",
		"c0-",
	}

	configuredShards := []string{"80-c0"}
	shardStates, err := psc.GetInitialState("connect-test", shards)
	assert.NoError(t, err)
	assert.Equal(t, len(configuredShards), len(shardStates.Shards))

	expectedShardStates := ShardStates{
		Shards: map[string]*SerializedCursor{},
	}

	for _, shard := range configuredShards {
		cursor, err := TableCursorToSerializedCursor(&psdbconnect.TableCursor{
			Shard:    shard,
			Keyspace: "connect-test",
			Position: "",
		})
		assert.NoError(t, err)
		expectedShardStates.Shards[shard] = cursor
	}

	assert.NoError(t, err)
	assert.Equal(t, expectedShardStates, shardStates)
}

func TestCanGenerateInitialState_Unsharded(t *testing.T) {
	psc := PlanetScaleSource{
		Host:     "useast.psdb.connect",
		Username: "usernameus-east-4",
		Password: "pscale_password",
		Database: "connect-test",
	}

	shards := []string{
		"-",
	}
	shardStates, err := psc.GetInitialState("connect-test", shards)
	assert.NoError(t, err)
	expectedShardStates := ShardStates{
		Shards: map[string]*SerializedCursor{},
	}

	for _, shard := range shards {
		cursor, err := TableCursorToSerializedCursor(&psdbconnect.TableCursor{
			Shard:    shard,
			Keyspace: "connect-test",
			Position: "",
		})
		assert.NoError(t, err)
		expectedShardStates.Shards[shard] = cursor
	}

	assert.NoError(t, err)
	assert.Equal(t, expectedShardStates, shardStates)
}

func TestCanGenerateInitialState_StartingGtids(t *testing.T) {
	psc := PlanetScaleSource{
		Host:          "useast.psdb.connect",
		Username:      "usernameus-east-4",
		Password:      "pscale_password",
		Database:      "connect-test",
		StartingGtids: "{\"connect-test\":{\"-40\":\"MySQL56/MYGTID1:1-3\",\"40-80\":\"MySQL56/MYGTID2:1-3\",\"80-c0\":\"MySQL56/MYGTID3:1-3\",\"c0-\":\"MySQL56/MYGTID4:1-3\"}}",
	}
	shards := []string{
		"-40",
		"40-80",
		"80-c0",
		"c0-",
	}
	shardStates, err := psc.GetInitialState("connect-test", shards)
	assert.NoError(t, err)
	expectedShardStates := ShardStates{
		Shards: map[string]*SerializedCursor{},
	}

	for i, shard := range shards {
		cursor, err := TableCursorToSerializedCursor(&psdbconnect.TableCursor{
			Shard:    shard,
			Keyspace: "connect-test",
			Position: fmt.Sprintf("MySQL56/MYGTID%s:1-3", fmt.Sprint(i+1)),
		})
		assert.NoError(t, err)
		expectedShardStates.Shards[shard] = cursor
	}

	assert.NoError(t, err)
	assert.Equal(t, expectedShardStates, shardStates)
}
