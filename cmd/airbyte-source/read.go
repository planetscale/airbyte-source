package airbyte_source

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/planetscale/airbyte-source/lib"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"

	"vitess.io/vitess/go/sqltypes"

	"github.com/planetscale/airbyte-source/cmd/internal"
	"github.com/spf13/cobra"
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
			ch.Logger = internal.NewSerializer(cmd.OutOrStdout())
			if readSourceConfigFilePath == "" {
				fmt.Fprintf(cmd.ErrOrStderr(), "Please pass path to a valid source config file via the [%v] argument", "config")
				return
			}

			if readSourceCatalogPath == "" {
				fmt.Fprintf(cmd.OutOrStdout(), "Please pass path to a valid source catalog file via the [%v] argument", "catalog")
				return
			}

			psc, err := parseSource(ch.FileReader, readSourceConfigFilePath)
			if err != nil {
				fmt.Fprintln(cmd.OutOrStdout(), "Please provide path to a valid configuration file")
				return
			}

			if err := ch.EnsureConnect(*psc); err != nil {
				fmt.Fprintln(cmd.OutOrStdout(), "Unable to connect to PlanetScale Database")
				return
			}

			defer func() {
				if ch.Mysql != nil {
					if err := ch.Mysql.Close(); err != nil {
						fmt.Fprintf(cmd.OutOrStdout(), "Unable to close connection to PlanetScale Database, failed with %v", err)
					}
				}
			}()

			cs, err := checkConnectionStatus(ch.Connect, psc)
			if err != nil {
				ch.Logger.ConnectionStatus(cs)
				return
			}

			catalog, err := readCatalog(ch.FileReader, readSourceCatalogPath)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to read catalog: %v", err))
				return
			}

			if len(catalog.Streams) == 0 {
				ch.Logger.Log(internal.LOGLEVEL_ERROR, "catalog has no streams")
				return
			}

			state := ""
			if stateFilePath != "" {
				b, err := os.ReadFile(stateFilePath)
				if err != nil {
					ch.Logger.Error(fmt.Sprintf("Unable to read state : %v", err))
					return
				}
				state = string(b)
			}

			shards, err := ch.Connect.ListShards(context.Background(), *psc)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to list shards : %v", err))
				return
			}

			syncState, err := readState(state, psc, catalog.Streams, shards)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to read state : %v", err))
				return
			}

			for _, table := range catalog.Streams {
				keyspaceOrDatabase := table.Stream.Namespace
				if keyspaceOrDatabase == "" {
					keyspaceOrDatabase = psc.Database
				}

				streamStateKey := keyspaceOrDatabase + ":" + table.Stream.Name
				streamState, ok := syncState.Keyspaces[keyspaceOrDatabase].Streams[streamStateKey]
				if !ok {
					ch.Logger.Error(fmt.Sprintf("Unable to read state for stream %v", streamStateKey))
					return
				}

				for shardName, shardState := range streamState.Shards {
					tc, err := shardState.SerializedCursorToTableCursor()
					if err != nil {
						ch.Logger.Error(fmt.Sprintf("invalid cursor for stream %v, failed with [%v]", streamStateKey, err))
						return
					}

					onResult := func(data *sqltypes.Result) error {
						rows := internal.QueryResultToRecords(data)
						for _, row := range rows {
							ch.Logger.Record(keyspaceOrDatabase, table.Stream.Name, row)
						}
						return nil
					}

					onCursor := func(tc *psdbconnect.TableCursor) error {
						sc, err := lib.TableCursorToSerializedCursor(tc)
						if err != nil {
							return err
						}
						syncState.Keyspaces[keyspaceOrDatabase].Streams[streamStateKey].Shards[shardName] = sc
						return nil
					}

					lps := lib.PlanetScaleSource{}

					sc, err := ch.Connect.Read(context.Background(), ch.Logger, lps, table.Stream.Name, tc, onResult, onCursor)
					// ch.Database.Read(context.Background(), cmd.OutOrStdout(), psc, table, tc)
					if err != nil {
						ch.Logger.Error(err.Error())
						return
					}

					if sc != nil {
						// if we get any new state, we assign it here.
						// otherwise, the older state is round-tripped back to Airbyte.
						syncState.Keyspaces[keyspaceOrDatabase].Streams[streamStateKey].Shards[shardName] = sc
					}
					ch.Logger.State(syncState)
					ch.Logger.Flush()
				}
			}
		},
	}
	readCmd.Flags().StringVar(&readSourceCatalogPath, "catalog", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&readSourceConfigFilePath, "config", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&stateFilePath, "state", "", "Path to the PlanetScale state information")
	return readCmd
}

type State struct {
	Shards map[string]map[string]interface{} `json:"shards"`
}

func readState(state string, psc *lib.PlanetScaleSource, streams []internal.ConfiguredStream, shards []string) (lib.SyncState, error) {
	syncState := lib.SyncState{
		Keyspaces: map[string]lib.KeyspaceState{
			psc.Database: {
				Streams: map[string]lib.ShardStates{},
			},
		},
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
		if _, ok := syncState.Keyspaces[psc.Database].Streams[stateKey]; !ok || ignoreCurrentCursor {
			initialState, err := psc.GetInitialState(keyspaceOrDatabase, shards)
			if err != nil {
				return syncState, err
			}
			syncState.Keyspaces[psc.Database].Streams[stateKey] = initialState
		}
	}

	return syncState, nil
}

func readCatalog(fr FileReader, path string) (c internal.ConfiguredCatalog, err error) {
	b, err := fr.ReadFile(path)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(b, &c)
	return c, err
}
