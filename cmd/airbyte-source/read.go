package airbyte_source

import (
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
					ch.Logger.Error("Unable to read state")
					os.Exit(1)
				}
				state = string(b)
			}

			states, err := readState(state, psc, catalog.Streams)
			if err != nil {
				ch.Logger.Error("Unable to read state")
				os.Exit(1)
			}

			cursorMap := make(map[string]interface{}, len(catalog.Streams))
			for _, table := range catalog.Streams {
				keyspaceOrDatabase := table.Stream.Namespace
				if keyspaceOrDatabase == "" {
					keyspaceOrDatabase = psc.Database
				}
				stateKey := keyspaceOrDatabase + ":" + table.Stream.Name
				state, ok := states[stateKey]
				if !ok {
					ch.Logger.Error(fmt.Sprintf("Unable to read state for stream %v", stateKey))
					os.Exit(1)
				}

				sc, err := psc.Read(cmd.OutOrStdout(), table, state)
				if err != nil {
					ch.Logger.Error(err.Error())
					os.Exit(1)
				}

				if sc == nil {
					// if we didn't get a cursor back, there might be no new rows yet
					// output the last known state again so that the next sync can pickup where this left off.
					cursorMap[stateKey] = serializeCursor(state)
				} else {
					cursorMap[stateKey] = sc
				}

				ch.Logger.State(cursorMap)
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

func readState(state string, psc internal.PlanetScaleConnection, streams []internal.ConfiguredStream) (map[string]*psdbdatav1.TableCursor, error) {
	states := map[string]*psdbdatav1.TableCursor{}
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

		emptyState := &psdbdatav1.TableCursor{
			Shard:    "-",
			Keyspace: keyspaceOrDatabase,
			Position: "",
		}
		states[stateKey] = emptyState

		if s.IncrementalSyncRequested() {
			if cursor, ok := tc[stateKey]; ok {
				var tc psdbdatav1.TableCursor
				err := json.Unmarshal([]byte(cursor.Cursor), &tc)
				if err != nil {
					return nil, err
				}
				states[stateKey] = &tc
			}
		}
	}

	return states, nil
}

func readCatalog(path string) (c internal.ConfiguredCatalog, err error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(b, &c)
	return c, err
}
