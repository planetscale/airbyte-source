package airbyte_source

import (
	"fmt"
	"os"

	"github.com/planetscale/connect/source/cmd/internal"
	"github.com/spf13/cobra"
)

var sourceConfigFilePath string

func init() {
	rootCmd.AddCommand(DiscoverCommand(DefaultHelper(os.Stdout)))
}

func DiscoverCommand(ch *Helper) *cobra.Command {
	discoverCmd := &cobra.Command{
		Use:   "discover",
		Short: "Discovers the schema for a PlanetScale database",
		Run: func(cmd *cobra.Command, args []string) {
			if sourceConfigFilePath == "" {
				fmt.Fprintln(cmd.OutOrStdout(), "Please provide path to a valid configuration file")
				return
			}

			cs, psc, err := checkConnectionStatus(ch.Database, ch.FileReader, sourceConfigFilePath)
			if err != nil {
				ch.Logger.ConnectionStatus(cs)
				return
			}

			c, err := psc.DiscoverSchema()
			if err != nil {
				ch.Logger.Log(internal.LOGLEVEL_ERROR, fmt.Sprintf("Unable to discover database, failed with [%v]", err))
				return
			}

			ch.Logger.Catalog(c)
		},
	}

	discoverCmd.Flags().StringVar(&sourceConfigFilePath, "config", "", "Path to the PlanetScale source configuration")
	return discoverCmd
}
