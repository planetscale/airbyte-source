package airbyte_source

import (
	"encoding/json"
	"fmt"
	"github.com/planetscale/connect/source/cmd/internal"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(SpecCommand())
}

func SpecCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "spec",
		Short: "Describes inputs needed for connecting to PlanetScale databases",
		Run: func(cmd *cobra.Command, args []string) {
			specMessage := internal.SpecMessage{
				Type: "SPEC",
				Spec: internal.Spec{
					DocumentationURL: "https://docs.airbyte.io/integrations/sources/mysql",
					SupportedDestinationSyncModes: []string{
						"overwrite",
					},
					SupportsIncremental: true,
					ConnectionSpecification: internal.ConnectionSpecification{
						Schema:   "http://json-schema.org/draft-07/schema#",
						Type:     "object",
						Title:    "PlanetScale Source Spec",
						Required: []string{"host", "database", "username", "password"},
						Properties: internal.ConnectionProperties{
							Host: internal.ConnectionProperty{
								Description: "The host name of the database.",
								Title:       "Host",
								Type:        "string",
								Order:       0,
							},
							Database: internal.ConnectionProperty{
								Title:       "Database",
								Description: "The PlanetScale database name.",
								Type:        "string",
								Order:       1,
							},
							Username: internal.ConnectionProperty{
								Description: "The username which is used to access the database.",
								Title:       "Username",
								Type:        "string",
								Order:       2,
							},
							Password: internal.ConnectionProperty{
								Description: "The password associated with the username.",
								Title:       "Password",
								Type:        "string",
								Order:       3,
								IsSecret:    true,
							},
							Shards: internal.ConnectionProperty{
								Description: "Comma separated list of shards you'd like to sync, by default all shards are synced.",
								Title:       "Shards",
								Type:        "string",
								Order:       4,
							},
							SyncDuration: internal.ConnectionProperty{
								Description: "Duration of each sync session in minutes",
								Title:       "Sync Duration",
								Type:        "integer",
								Maximum:     20,
								Minimum:     0,
								Order:       5,
							},
						},
					},
				},
			}

			msg, _ := json.Marshal(specMessage)
			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", string(msg))
		},
	}
}
