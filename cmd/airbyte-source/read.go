package airbyte_source

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/planetscale/airbyte-source/cmd/internal"
	psdbconnectv1alpha1 "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
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
			ch.Logger = internal.NewLogger(cmd.OutOrStdout())
			if readSourceConfigFilePath == "" {
				fmt.Fprintf(cmd.ErrOrStderr(), "Please pass path to a valid source config file via the [%v] argument", "config")
				os.Exit(1)
			}

			if readSourceCatalogPath == "" {
				fmt.Fprintf(cmd.OutOrStdout(), "Please pass path to a valid source catalog file via the [%v] argument", "config")
				os.Exit(1)
			}

			ch.Logger.Log(internal.LOGLEVEL_INFO, "Checking connection")

			psc, err := parseSource(ch.FileReader, readSourceConfigFilePath)
			if err != nil {
				fmt.Fprintln(cmd.OutOrStdout(), "Please provide path to a valid configuration file")
				return
			}

			if err := ch.EnsureDB(psc); err != nil {
				fmt.Fprintln(cmd.OutOrStdout(), "Unable to connect to PlanetScale Database")
				return
			}

			defer func() {
				if err := ch.Database.Close(); err != nil {
					fmt.Fprintf(cmd.OutOrStdout(), "Unable to close connection to PlanetScale Database, failed with %v", err)
				}
			}()

			cs, err := checkConnectionStatus(ch.Database, psc)
			if err != nil {
				ch.Logger.ConnectionStatus(cs)
				return
			}

			catalog, err := readCatalog(readSourceCatalogPath)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to read catalog: %+v", err))
				os.Exit(1)
			}

			if len(catalog.Streams) == 0 {
				ch.Logger.Log(internal.LOGLEVEL_ERROR, "Catalog has no streams")
				return
			}

			state := ""
			if stateFilePath != "" {
				ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("State file detected, parsing provided file %s", stateFilePath))
				b, err := os.ReadFile(stateFilePath)
				if err != nil {
					ch.Logger.Error(fmt.Sprintf("Unable to read state : %v", err))
					os.Exit(1)
				}
				state = string(b)
			}
			shards, err := ch.Database.ListShards(context.Background(), psc)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to list shards : %v", err))
				os.Exit(1)
			}

			syncState, err := readState(state, psc, catalog.Streams, shards, ch.Logger)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to read state : %v", err))
				os.Exit(1)
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
					var tc *psdbconnectv1alpha1.TableCursor

					tc, err = shardState.SerializedCursorToTableCursor(table)
					ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Using serialized cursor for stream %s", streamStateKey))
					if err != nil {
						ch.Logger.Error(fmt.Sprintf("Invalid serialized cursor for stream %v, failed with [%v]", streamStateKey, err))
						os.Exit(1)
					}

					sc, err := ch.Database.Read(context.Background(), cmd.OutOrStdout(), psc, table, tc)
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

type State struct {
	Shards map[string]map[string]interface{} `json:"shards"`
}

func readState(state string, psc internal.PlanetScaleSource, streams []internal.ConfiguredStream, shards []string, logger internal.AirbyteLogger) (internal.SyncState, error) {
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
		logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Syncing stream %s with sync mode %s", s.Stream.Name, s.SyncMode))
		ignoreCurrentCursor := !s.IncrementalSyncRequested()

		// if no table cursor was found in the state, or we want to ignore the current cursor,
		// Send along an empty cursor for each shard.
		if _, ok := syncState.Streams[stateKey]; !ok || ignoreCurrentCursor {
			logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Ignoring current cursor since incremental sync is disabled, or no cursor was found for key %s", stateKey))
			initialState, err := psc.GetInitialState(keyspaceOrDatabase, shards)
			if err != nil {
				return syncState, err
			}
			syncState.Streams[stateKey] = initialState
		}
	}

	return syncState, nil
}

func readCatalog(path string) (c internal.ConfiguredCatalog, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(b, &c)
	return c, err
}
