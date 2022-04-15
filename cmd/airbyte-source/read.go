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

			states, err := readState(state, psc, catalog.Streams)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to read state : %v", err))
				os.Exit(1)
			}

			fmt.Printf("\n \t found states : %v\n", states)
			quit := false
			if quit {
				os.Exit(0)
			}

			fmt.Printf("\n\t found states : [%v]\n", states)
			cursorMap := make(map[string]map[string]interface{}, len(catalog.Streams))
			for _, table := range catalog.Streams {
				keyspaceOrDatabase := table.Stream.Namespace
				if keyspaceOrDatabase == "" {
					keyspaceOrDatabase = psc.Database
				}
				stateKey := keyspaceOrDatabase + ":" + table.Stream.Name
				states, ok := states[stateKey]
				if !ok {
					ch.Logger.Error(fmt.Sprintf("Unable to read state for stream %v", stateKey))
					os.Exit(1)
				}

				for _, state := range states {
					sc, err := psc.Read(cmd.OutOrStdout(), table, state)
					if err != nil {
						ch.Logger.Error(err.Error())
						os.Exit(1)
					}

					if cursorMap[stateKey] == nil {
						cursorMap[stateKey] = map[string]interface{}{}
					}

					if sc == nil {
						// if we didn't get a cursor back, there might be no new rows yet
						// output the last known state again so that the next sync can pickup where this left off.
						cursorMap[stateKey][state.Shard] = serializeCursor(state)

					} else {
						cursorMap[stateKey][state.Shard] = sc
					}

					ch.Logger.State(cursorMap)
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

func readState(state string, psc internal.PlanetScaleConnection, streams []internal.ConfiguredStream) (map[string][]*psdbdatav1.TableCursor, error) {
	states := map[string][]*psdbdatav1.TableCursor{}
	var tc map[string]internal.SerializedCursor
	if state != "" {
		err := json.Unmarshal([]byte(state), &tc)
		if err != nil {
			return nil, err
		}
	}

	for _, s := range streams {

		keyspaceOrDatabase := s.Stream.Namespace
		if keyspaceOrDatabase == "" {
			keyspaceOrDatabase = psc.Database
		}
		stateKey := keyspaceOrDatabase + ":" + s.Stream.Name

		if s.IncrementalSyncRequested() {
			if cursor, ok := tc[stateKey]; ok {
				var tc psdbdatav1.TableCursor
				err := json.Unmarshal([]byte(cursor.Cursor), &tc)
				if err != nil {
					return nil, err
				}
				states[stateKey] = append(states[stateKey], &tc)
			}
		}

		// if no table cursor was found in the state,
		// Send along an empty cursor for each shard.
		if len(states[stateKey]) == 0 {
			emptyCursors, err := getEmptyShardCursors(psc, keyspaceOrDatabase)
			if err != nil {
				return states, err
			}

			states[stateKey] = emptyCursors
		}
	}

	return states, nil
}

func getEmptyShardCursors(psc internal.PlanetScaleConnection, keyspaceOrDatabase string) ([]*psdbdatav1.TableCursor, error) {
	var cursors []*psdbdatav1.TableCursor
	shards, err := psc.ListShards(context.Background())
	if err != nil {
		return cursors, err
	}

	for _, shard := range shards {
		cursors = append(cursors, &psdbdatav1.TableCursor{
			Shard:    shard,
			Keyspace: keyspaceOrDatabase,
			Position: "",
		})
	}

	return cursors, nil
}

func readCatalog(path string) (c internal.ConfiguredCatalog, err error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(b, &c)
	return c, err
}
