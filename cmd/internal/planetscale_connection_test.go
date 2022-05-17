package internal

import (
	psdbconnect "github.com/planetscale/edge-gateway/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/assert"
	"os"
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
	os.Setenv("PS_END_TO_END_TEST_RUN", "true")
	defer os.Unsetenv("PS_END_TO_END_TEST_RUN")
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

	expectedShardStates := ShardStates{
		Shards: map[string]*SerializedCursor{},
	}

	for _, shard := range shards {
		expectedShardStates.Shards[shard], _ = TableCursorToSerializedCursor(&psdbconnect.TableCursor{
			Shard:    shard,
			Keyspace: "connect-test",
			Position: "",
		})
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

	expectedShardStates := ShardStates{
		Shards: map[string]*SerializedCursor{},
	}

	for _, shard := range shards {
		expectedShardStates.Shards[shard], _ = TableCursorToSerializedCursor(&psdbconnect.TableCursor{
			Shard:    shard,
			Keyspace: "connect-test",
			Position: "",
		})
	}

	assert.NoError(t, err)
	assert.Equal(t, expectedShardStates, shardStates)
}
