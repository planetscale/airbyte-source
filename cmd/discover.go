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
	discoverCmd.Flags().StringVar(&sourceConfigFilePath, "config", "", "Path to the PlanetScale source configuration")
	rootCmd.AddCommand(discoverCmd)
}

var discoverCmd = &cobra.Command{
	Use:   "discover",
	Short: "Discovers the schema for a PlanetScale database",
	Run: func(cmd *cobra.Command, args []string) {
		cs, psc, err := checkConfig(sourceConfigFilePath)
		if err != nil {
			printConnectionStatus(cs, "Connection test failed", LOGLEVEL_ERROR)
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
