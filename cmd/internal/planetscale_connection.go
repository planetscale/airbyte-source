package internal

import (
	"fmt"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/connect-sdk/lib"
	"strings"
)

// PlanetScaleSource defines a configured Airbyte Source for a PlanetScale database
type PlanetScaleSource struct {
	Host       string              `json:"host"`
	Database   string              `json:"database"`
	Username   string              `json:"username"`
	Password   string              `json:"password"`
	Shards     string              `json:"shards"`
	UseReplica bool                `json:"use_replica"`
	Options    CustomSourceOptions `json:"options"`
}

type CustomSourceOptions struct {
	DoNotTreatTinyIntAsBoolean bool `json:"do_not_treat_tiny_int_as_boolean"`
}

// GetInitialState will return the initial/blank state for a given keyspace in all of its shards.
// This state can be round-tripped safely with Airbyte.
func (psc PlanetScaleSource) GetInitialState(keyspaceOrDatabase string, shards []string) (ShardStates, error) {
	shardCursors := ShardStates{
		Shards: map[string]*lib.SerializedCursor{},
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
