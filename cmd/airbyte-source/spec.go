package airbyte_source

import (
	"encoding/json"
	"fmt"

	"github.com/planetscale/airbyte-source/cmd/internal"
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
					DocumentationURL: "https://docs.planetscale.com/integrations/airbyte",
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
							Options: internal.CustomOptionsSpecification{
								Order:       5,
								Type:        "object",
								Title:       "Custom configuration options",
								Description: "Configuration options to customize PlanetScale source",
								Options: []internal.CustomOptions{
									{
										Type:        "object",
										Title:       "options",
										Description: "options",
										Properties: internal.CustomOptionsProperties{
											DoNotTreatTinyIntAsBoolean: internal.ConnectionProperty{
												Description: "If enabled, properties of type TinyInt(1) are output as TinyInt, and not boolean.",
												Title:       "Do Not Treat TinyInt(1) as boolean",
												Type:        "boolean",
												Order:       5,
											},
										},
									},
								},
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
