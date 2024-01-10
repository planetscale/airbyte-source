package airbyte_source

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/planetscale/airbyte-source/cmd/internal"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/connect-sdk/lib"
	"github.com/spf13/cobra"
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

			cs, err := checkConnectionStatus(ch.ConnectClient, ch.Source)
			if err != nil {
				ch.Logger.ConnectionStatus(cs)
				return
			}

			catalog, err := readCatalog(readSourceCatalogPath)
			if err != nil {
				ch.Logger.Error("Unable to read catalog")
				os.Exit(1)
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
					os.Exit(1)
				}
				state = string(b)
			}

			shards, err := ch.ConnectClient.ListShards(context.Background(), ch.Source)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to list shards : %v", err))
				os.Exit(1)
			}

			syncState, err := readState(state, psc, catalog.Streams, shards)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to read state : %v", err))
				os.Exit(1)
			}

			allColumns := []string{}
			rb := internal.NewResultBuilder(ch.Logger)
			irb := rb.(*internal.ResultBuilder)
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
				irb.SetKeyspace(keyspaceOrDatabase)
				irb.SetTable(table.Stream.Name)

				for shardName, shardState := range streamState.Shards {
					tc, err := shardState.DeserializeTableCursor()
					if err != nil {
						ch.Logger.Error(fmt.Sprintf("invalid cursor for stream %v, failed with [%v]", streamStateKey, err))
						os.Exit(1)
					}
					irb.HandleOnCursor = func(tc *psdbconnect.TableCursor) error {
						sc, err := lib.SerializeTableCursor(tc)
						if err != nil {
							return err
						}
						syncState.Streams[streamStateKey].Shards[shardName] = sc
						ch.Logger.State(syncState)
						ch.Logger.Flush()
						return nil
					}

					sc, err := ch.ConnectClient.Read(context.Background(), ch.Logger, ch.Source, table.Stream.Name, allColumns, tc, rb)
					if err != nil {
						ch.Logger.Error(err.Error())
						os.Exit(1)
					}
					if sc != nil {
						// if we get any new state, we assign it here.
						// otherwise, the older state is round-tripped back to Airbyte.
						syncState.Streams[streamStateKey].Shards[shardName] = sc
					}

					ch.Logger.Flush()
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

func readState(state string, psc internal.PlanetScaleSource, streams []internal.ConfiguredStream, shards []string) (internal.SyncState, error) {
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
