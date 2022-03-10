package airbyte_source

import (
	"encoding/json"
	"fmt"
	"github.com/planetscale/connect/source/cmd/internal"
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

			for _, table := range catalog.Streams {
				err := psc.Read(cmd.OutOrStdout(), table.Stream, state)
				if err != nil {
					ch.Logger.Error(cmd.OutOrStdout(), err.Error())
					os.Exit(1)
				}

			}
		},
	}
	readCmd.Flags().StringVar(&readSourceCatalogPath, "catalog", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&readSourceConfigFilePath, "config", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&stateFilePath, "state", "", "Path to the PlanetScale state information")
	return readCmd
}

func readCatalog(path string) (c internal.ConfiguredCatalog, err error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(b, &c)
	return c, err
}
