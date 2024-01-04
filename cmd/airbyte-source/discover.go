package airbyte_source

import (
	"context"
	"fmt"
	"os"

	"github.com/planetscale/airbyte-source/cmd/internal"
	"github.com/planetscale/connectsdk/lib"
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

			if err := ch.EnsureDB(psc); err != nil {
				fmt.Fprintln(cmd.OutOrStdout(), "Unable to connect to PlanetScale Database")
				return
			}

			cs, err := checkConnectionStatus(psc)
			if err != nil {
				ch.Logger.ConnectionStatus(cs)
				return
			}

			defer func() {
				if err := ch.Database.Close(); err != nil {
					fmt.Fprintf(cmd.OutOrStdout(), "Unable to close connection to PlanetScale Database, failed with %v", err)
				}
			}()

			libpsc := lib.PlanetScaleSource{
				UseReplica:            true,
				Username:              psc.Username,
				Database:              psc.Database,
				Host:                  psc.Host,
				Password:              psc.Password,
				TreatTinyIntAsBoolean: !psc.Options.DoNotTreatTinyIntAsBoolean,
			}
			mc, err := lib.NewMySQL(&libpsc)
			if err != nil {
				ch.Logger.Log(internal.LOGLEVEL_ERROR, fmt.Sprintf("Unable to discover database, failed with [%v]", err))
				return
			}

			sb := internal.NewSchemaBuilder()
			if err := mc.BuildSchema(context.Background(), libpsc, sb); err != nil {
				ch.Logger.Log(internal.LOGLEVEL_ERROR, fmt.Sprintf("Unable to discover database, failed with [%v]", err))
				return
			}

			c := sb.(*internal.SchemaBuilder).GetCatalog()
			ch.Logger.Catalog(c)
		},
	}

	discoverCmd.Flags().StringVar(&sourceConfigFilePath, "config", "", "Path to the PlanetScale source configuration")
	return discoverCmd
}
