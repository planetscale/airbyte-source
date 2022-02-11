package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"io/ioutil"
)

var (
	readSourceConfigFilePath string
	readSourceCatalogPath    string
	stateFilePath            string
)

func init() {
	rootCmd.AddCommand(ReadCommand())
}

func ReadCommand() *cobra.Command {
	readCmd := &cobra.Command{
		Use:   "read",
		Short: "Converts rows from a PlanetScale database into AirbyteRecordMessages",
		Run: func(cmd *cobra.Command, args []string) {
			cs, psc, err := checkConfig(readSourceConfigFilePath)
			if err != nil {
				printConnectionStatus(cs, "Connection test failed", LOGLEVEL_ERROR)
			}

			catalog, err := readCatalog(readSourceCatalogPath)
			if err != nil {
				fmt.Println("Unable to read catalog")
				//return err
			}

			state := ""
			if stateFilePath != "" {
				b, err := ioutil.ReadFile(stateFilePath)
				if err != nil {
					fmt.Println("Unable to read state")
				}
				state = string(b)
			}

			for _, table := range catalog.Streams {
				psc.Read(table.Stream, state)
			}
		},
	}
	readCmd.Flags().StringVar(&readSourceCatalogPath, "catalog", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&readSourceConfigFilePath, "config", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&stateFilePath, "state", "", "Path to the PlanetScale state information")
	return readCmd
}

func readCatalog(path string) (c ConfiguredCatalog, err error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(b, &c)
	return c, err
}
