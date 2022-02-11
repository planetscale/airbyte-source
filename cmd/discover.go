package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
)

var (
	sourceConfigFilePath string
)

func init() {
	rootCmd.AddCommand(DiscoverCommand(DefaultHelper()))
}

func DiscoverCommand(ch *Helper) *cobra.Command {

	discoverCmd := &cobra.Command{
		Use:   "discover",
		Short: "Discovers the schema for a PlanetScale database",
		Run: func(cmd *cobra.Command, args []string) {
			if sourceConfigFilePath == "" {
				fmt.Fprintf(cmd.OutOrStdout(), "Please pass path to a valid source config file via the [%v] argument", "config")
				return
			}
			cs, psc, err := checkConfig(ch.Database, ch.FileReader, sourceConfigFilePath)
			if err != nil {
				printConnectionStatus(cmd.OutOrStdout(), cs, "Connection test failed", LOGLEVEL_ERROR)
			}

			c, err := psc.DiscoverSchema()
			if err != nil {
			}

			msg, _ := json.Marshal(AirbyteMessage{
				Type:    CATALOG,
				Catalog: &c,
				Log: &AirbyteLogMessage{
					Level:   LOGLEVEL_INFO,
					Message: "Retrieved schema successfully",
				},
			})
			fmt.Println(string(msg))
		},
	}

	discoverCmd.Flags().StringVar(&sourceConfigFilePath, "config", "", "Path to the PlanetScale source configuration")
	return discoverCmd
}
