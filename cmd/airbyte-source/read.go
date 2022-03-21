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
	rootCmd.AddCommand(ReadCommand(DefaultHelper()))
}

func ReadCommand(ch *Helper) *cobra.Command {
	readCmd := &cobra.Command{
		Use:   "read",
		Short: "Converts rows from a PlanetScale database into AirbyteRecordMessages",
		Run: func(cmd *cobra.Command, args []string) {
			if readSourceConfigFilePath == "" {
				fmt.Fprintf(cmd.OutOrStdout(), "Please pass path to a valid source config file via the [%v] argument", "config")
				return
			}

			if readSourceCatalogPath == "" {
				fmt.Fprintf(cmd.OutOrStdout(), "Please pass path to a valid source catalog file via the [%v] argument", "config")
				return
			}

			cs, psc, err := checkConnectionStatus(ch.Database, ch.FileReader, readSourceConfigFilePath)
			if err != nil {
				printConnectionStatus(cmd.OutOrStdout(), cs, "Connection test failed", internal.LOGLEVEL_ERROR)
			}

			catalog, err := readCatalog(readSourceCatalogPath)
			if err != nil {
				ch.Logger.Error(cmd.OutOrStdout(), "Unable to read catalog")
				os.Exit(1)
			}

			state := ""
			if stateFilePath != "" {
				b, err := ioutil.ReadFile(stateFilePath)
				if err != nil {
					ch.Logger.Error(cmd.OutOrStdout(), "Unable to read state")
					os.Exit(1)
				}
				state = string(b)
			}

			states, err := readState(state, psc, catalog.Streams)
			if err != nil {
				ch.Logger.Error(cmd.OutOrStdout(), "Unable to read state")
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
					ch.Logger.Error(cmd.OutOrStdout(), fmt.Sprintf("Unable to read state for stream %v", stateKey))
					os.Exit(1)
				}

				sc, err := psc.Read(cmd.OutOrStdout(), table.Stream, state)
				if err != nil {
					ch.Logger.Error(cmd.OutOrStdout(), err.Error())
					os.Exit(1)
				}

				if sc != nil {
					cursorMap[stateKey] = sc
				}

				ch.Logger.State(cmd.OutOrStdout(), cursorMap)
			}

		},
	}
	readCmd.Flags().StringVar(&readSourceCatalogPath, "catalog", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&readSourceConfigFilePath, "config", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&stateFilePath, "state", "", "Path to the PlanetScale state information")
	return readCmd
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

		cursor, ok := tc[stateKey]
		if !ok {
			// if no known cursor for this stream, make an empty one.
			states[stateKey] = &psdbdatav1.TableCursor{
				Shard:    "-",
				Keyspace: keyspaceOrDatabase,
				Position: "",
			}
		} else {
			var tc psdbdatav1.TableCursor
			err := json.Unmarshal([]byte(cursor.Cursor), &tc)
			if err != nil {
				return nil, err
			}
			states[stateKey] = &tc
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
