package internal

import (
	"fmt"
	"github.com/go-sql-driver/mysql"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"os"
	"strings"
)

// PlanetScaleSource defines a configured Airbyte Source for a PlanetScale database
type PlanetScaleSource struct {
	Host     string `json:"host"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
	Shards   string `json:"shards"`
}

// DSN returns a DataSource that mysql libraries can use to connect to a PlanetScale database.
func (psc PlanetScaleSource) DSN(tt psdbconnect.TabletType) string {
	config := mysql.NewConfig()
	config.Net = "tcp"
	config.Addr = psc.Host
	config.User = psc.Username
	config.DBName = psc.Database
	config.Passwd = psc.Password

	if useSecureConnection() {
		config.TLSConfig = "true"
		config.DBName = fmt.Sprintf("%v@%v", psc.Database, TabletTypeToString(tt))
	} else {
		config.TLSConfig = "skip-verify"
	}
	return config.FormatDSN()
}

// GetInitialState will return the initial/blank state for a given keyspace in all of its shards.
// This state can be round-tripped safely with Airbyte.
func (psc PlanetScaleSource) GetInitialState(keyspaceOrDatabase string, shards []string) (ShardStates, error) {
	shardCursors := ShardStates{
		Shards: map[string]*SerializedCursor{},
	}

	if len(psc.Shards) > 0 {
		configuredShards := strings.Split(psc.Shards, ",")
		foundShards := map[string]bool{}
		for _, existingShard := range shards {
			foundShards[existingShard] = true
		}

		for _, configuredShard := range configuredShards {
			if len(configuredShard) > 0 {
				if _, ok := foundShards[strings.TrimSpace(configuredShard)]; !ok {
					return shardCursors, fmt.Errorf("shard %v does not exist on the source database", configuredShard)
				}
			}
		}

		// if we got this far, all the shards that the customer asked for exist in the PlanetScale database.
		shards = configuredShards
	}

	for _, shard := range shards {
		shardCursors.Shards[shard], _ = TableCursorToSerializedCursor(&psdbconnect.TableCursor{
			Shard:    shard,
			Keyspace: keyspaceOrDatabase,
			Position: "",
		})
	}

	return shardCursors, nil
}

func useSecureConnection() bool {
	e2eTestRun, found := os.LookupEnv("PS_END_TO_END_TEST_RUN")
	if found && (e2eTestRun == "yes" ||
		e2eTestRun == "y" ||
		e2eTestRun == "true" ||
		e2eTestRun == "1") {
		return false
	}

	return true
}

func TabletTypeToString(t psdbconnect.TabletType) string {
	if t == psdbconnect.TabletType_replica {
		return "replica"
	}

	return "primary"
}
