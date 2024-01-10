package internal

import (
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/connect-sdk/lib"
	"github.com/stretchr/testify/assert"
	"testing"
)

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
		Shards: map[string]*lib.SerializedCursor{},
	}

	for _, shard := range shards {
		expectedShardStates.Shards[shard], err = TableCursorToSerializedCursor(&psdbconnect.TableCursor{
			Shard:    shard,
			Keyspace: "connect-test",
			Position: "",
		})
		assert.NoError(t, err)
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
		Shards: map[string]*lib.SerializedCursor{},
	}

	for _, shard := range configuredShards {
		expectedShardStates.Shards[shard], err = TableCursorToSerializedCursor(&psdbconnect.TableCursor{
			Shard:    shard,
			Keyspace: "connect-test",
			Position: "",
		})
		assert.NoError(t, err)
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
		Shards: map[string]*lib.SerializedCursor{},
	}

	for _, shard := range shards {
		expectedShardStates.Shards[shard], err = TableCursorToSerializedCursor(&psdbconnect.TableCursor{
			Shard:    shard,
			Keyspace: "connect-test",
			Position: "",
		})
		assert.NoError(t, err)
	}

	assert.NoError(t, err)
	assert.Equal(t, expectedShardStates, shardStates)
}
