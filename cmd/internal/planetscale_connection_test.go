package internal

import (
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCanGenerateSecureDSN(t *testing.T) {
	psc := PlanetScaleSource{
		Host:     "useast.psdb.connect",
		Username: "usernameus-east-4",
		Password: "pscale_password",
		Database: "connect-test",
	}
	dsn := psc.DSN(psdbconnect.TabletType_primary)
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
	dsn := psc.DSN(psdbconnect.TabletType_primary)
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
		Shards: map[string]*SerializedCursor{},
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
