package airbyte_source

import (
	"context"
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

			if err := ch.EnsureConnect(*psc); err != nil {
				fmt.Fprintln(cmd.OutOrStdout(), "Unable to connect to PlanetScale Database")
				return
			}

			cs, err := checkConnectionStatus(ch.Connect, psc)
			if err != nil {
				ch.Logger.ConnectionStatus(cs)
				return
			}

			defer func() {
				if ch.Mysql != nil {
					if err := ch.Mysql.Close(); err != nil {
						fmt.Fprintf(cmd.OutOrStdout(), "Unable to close connection to PlanetScale Database, failed with %v", err)
					}
				}
			}()

			sb := internal.NewSchemaBuilder(psc.TreatTinyIntAsBoolean)
			if err := ch.Mysql.BuildSchema(context.Background(), *psc, sb); err != nil {
				ch.Logger.Log(internal.LOGLEVEL_ERROR, fmt.Sprintf("Unable to discover database, failed with [%v]", err))
				return
			}

			c := sb.(*internal.AirbyteSchemaBuilder).BuildCatalog()
			ch.Logger.Catalog(*c)
		},
	}

	discoverCmd.Flags().StringVar(&sourceConfigFilePath, "config", "", "Path to the PlanetScale source configuration")
	return discoverCmd
}
