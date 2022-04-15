package airbyte_source

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/planetscale/connect/source/cmd/internal"
	psdbdatav1 "github.com/planetscale/edge-gateway/proto/psdb/data_v1"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
)

var (
	readSourceConfigFilePath string
	readSourceCatalogPath    string
	stateFilePath            string
)

func init() {
	rootCmd.AddCommand(ReadCommand(DefaultHelper(os.Stdout)))
}

func ReadCommand(ch *Helper) *cobra.Command {
	readCmd := &cobra.Command{
		Use:   "read",
		Short: "Converts rows from a PlanetScale database into AirbyteRecordMessages",
		Run: func(cmd *cobra.Command, args []string) {
			ch.Logger = internal.NewLogger(cmd.OutOrStdout())
			if readSourceConfigFilePath == "" {
				fmt.Fprintf(cmd.OutOrStdout(), "Please pass path to a valid source config file via the [%v] argument", "config")
				os.Exit(1)
			}

			if readSourceCatalogPath == "" {
				fmt.Fprintf(cmd.OutOrStdout(), "Please pass path to a valid source catalog file via the [%v] argument", "config")
				os.Exit(1)
			}

			cs, psc, err := checkConnectionStatus(ch.Database, ch.FileReader, readSourceConfigFilePath)
			if err != nil {
				printConnectionStatus(cmd.OutOrStdout(), cs, "Connection test failed", internal.LOGLEVEL_ERROR)
				os.Exit(1)
			}

			catalog, err := readCatalog(readSourceCatalogPath)
			if err != nil {
				ch.Logger.Error("Unable to read catalog")
				os.Exit(1)
			}

			state := ""
			if stateFilePath != "" {
				b, err := ioutil.ReadFile(stateFilePath)
				if err != nil {
					ch.Logger.Error(fmt.Sprintf("Unable to read state : %v", err))
					os.Exit(1)
				}
				state = string(b)
			}

			syncState, err := readState(state, psc, catalog.Streams)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to read state : %v", err))
				os.Exit(1)
			}

			quit := false
			if quit {
				os.Exit(0)
			}

			for _, table := range catalog.Streams {
				keyspaceOrDatabase := table.Stream.Namespace
				if keyspaceOrDatabase == "" {
					keyspaceOrDatabase = psc.Database
				}
				streamStateKey := keyspaceOrDatabase + ":" + table.Stream.Name
				streamState, ok := syncState.Streams[streamStateKey]
				if !ok {
					ch.Logger.Error(fmt.Sprintf("Unable to read state for stream %v", streamStateKey))
					os.Exit(1)
				}

				for shardName, shardState := range streamState.Shards {
					tc, err := shardState.ToTableCursor()
					if err != nil {
						ch.Logger.Error(fmt.Sprintf("invalid cursor for stream %v", streamStateKey))
						os.Exit(1)
					}
					sc, err := psc.Read(cmd.OutOrStdout(), table, tc)
					if err != nil {
						ch.Logger.Error(err.Error())
						os.Exit(1)
					}

					if sc != nil {
						// if we get any new state, we assign it here.
						// otherwise, the older state is round-tripped back to Airbyte.
						syncState.Streams[streamStateKey].Shards[shardName] = sc
					}
					ch.Logger.State(syncState)
				}
			}

		},
	}
	readCmd.Flags().StringVar(&readSourceCatalogPath, "catalog", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&readSourceConfigFilePath, "config", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&stateFilePath, "state", "", "Path to the PlanetScale state information")
	return readCmd
}

func serializeCursor(cursor *psdbdatav1.TableCursor) *internal.SerializedCursor {
	b, _ := json.Marshal(cursor)

	sc := &internal.SerializedCursor{
		Cursor: string(b),
	}
	return sc
}

type State struct {
	Shards map[string]map[string]interface{} `json:"shards"`
}

func readState(state string, psc internal.PlanetScaleConnection, streams []internal.ConfiguredStream) (internal.SyncState, error) {
	syncState := internal.SyncState{
		Streams: map[string]internal.ShardStates{},
	}
	if state != "" {
		err := json.Unmarshal([]byte(state), &syncState)
		if err != nil {
			return syncState, err
		}
	}

	for _, s := range streams {

		keyspaceOrDatabase := s.Stream.Namespace
		if keyspaceOrDatabase == "" {
			keyspaceOrDatabase = psc.Database
		}
		stateKey := keyspaceOrDatabase + ":" + s.Stream.Name
		ignoreCurrentCursor := !s.IncrementalSyncRequested()

		// if no table cursor was found in the state, or we want to ignore the current cursor,
		// Send along an empty cursor for each shard.
		if _, ok := syncState.Streams[stateKey]; !ok || ignoreCurrentCursor {
			emptyCursors, err := getEmptyShardCursors(psc, keyspaceOrDatabase)
			if err != nil {
				return syncState, err
			}
			syncState.Streams[stateKey] = emptyCursors
		}
	}

	return syncState, nil
}

func getEmptyShardCursors(psc internal.PlanetScaleConnection, keyspaceOrDatabase string) (internal.ShardStates, error) {
	shardCursors := internal.ShardStates{
		Shards: map[string]*internal.SerializedCursor{},
	}
	shards, err := psc.ListShards(context.Background())
	if err != nil {
		return shardCursors, err
	}

	for _, shard := range shards {
		shardCursors.Shards[shard] = serializeCursor(&psdbdatav1.TableCursor{
			Shard:    shard,
			Keyspace: keyspaceOrDatabase,
			Position: "",
		})
	}

	return shardCursors, nil
}

func readCatalog(path string) (c internal.ConfiguredCatalog, err error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(b, &c)
	return c, err
}
