package airbyte_source

import (
	"fmt"
	"os"

	"github.com/planetscale/airbyte-source/cmd/internal"
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
			ctx := cmd.Context()

			if sourceConfigFilePath == "" {
				fmt.Fprintln(cmd.OutOrStdout(), "Please provide path to a valid configuration file")
				return
			}

			psc, err := parseSource(ch.FileReader, sourceConfigFilePath)
			if err != nil {
				cs := internal.ConnectionStatus{
					Status:  "FAILED",
					Message: fmt.Sprintf("Configuration for PlanetScale database is invalid, unable to read source configuration : %v", err),
				}
				ch.Logger.ConnectionStatus(cs)
				return
			}

			if err := ch.EnsureDB(psc); err != nil {
				fmt.Fprintln(cmd.OutOrStdout(), "Unable to connect to PlanetScale Database")
				return
			}

			cs, err := checkConnectionStatus(ctx, ch.Database, psc)
			if err != nil {
				ch.Logger.ConnectionStatus(cs)
				return
			}

			defer func() {
				if err := ch.Database.Close(); err != nil {
					fmt.Fprintf(cmd.OutOrStdout(), "Unable to close connection to PlanetScale Database, failed with %v", err)
				}
			}()

			c, err := ch.Database.DiscoverSchema(ctx, psc)
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
