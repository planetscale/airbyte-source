package cmd

import (
	"encoding/json"
	"fmt"
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
			specMessage := SpecMessage{
				Type: "SPEC",
				Spec: Spec{
					DocumentationURL: "https://docs.airbyte.io/integrations/sources/mysql",
					ConnectionSpecification: ConnectionSpecification{
						Title:    "PlanetScale Source Spec",
						Required: []string{"host", "database", "username", "password"},
						Properties: ConnectionProperties{
							Host: ConnectionProperty{
								Description: "The host name of the database.",
								Title:       "Host",
								Type:        "string",
								Order:       0,
							},
							Database: ConnectionProperty{
								Title:       "Database",
								Description: "The PlanetScale database name.",
								Type:        "string",
								Order:       1,
							},
							Username: ConnectionProperty{
								Description: "The username which is used to access the database.",
								Title:       "Username",
								Type:        "string",
								Order:       2,
							},
							Password: ConnectionProperty{
								Description: "The password associated with the username.",
								Title:       "Password",
								Type:        "string",
								Order:       3,
								IsSecret:    true,
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

type SpecMessage struct {
	Type string `json:"type"`
	Spec Spec   `json:"spec"`
}

type ConnectionProperties struct {
	Host     ConnectionProperty `json:"host"`
	Database ConnectionProperty `json:"database"`
	Username ConnectionProperty `json:"username"`
	Password ConnectionProperty `json:"password"`
}

type ConnectionProperty struct {
	Description string `json:"description"`
	Title       string `json:"title"`
	Type        string `json:"type"`
	Order       int    `json:"order"`
	IsSecret    bool   `json:"airbyte_secret"`
}

type ConnectionSpecification struct {
	Schema               string               `json:"$schema"`
	Title                string               `json:"title"`
	Type                 string               `json:"type"`
	Required             []string             `json:"required"`
	AdditionalProperties bool                 `json:"additionalProperties"`
	Properties           ConnectionProperties `json:"properties"`
}

type Spec struct {
	DocumentationURL              string                  `json:"documentationUrl"`
	ConnectionSpecification       ConnectionSpecification `json:"connectionSpecification"`
	SupportsNormalization         bool                    `json:"supportsNormalization"`
	SupportsDBT                   bool                    `json:"supportsDBT"`
	SupportedDestinationSyncModes []interface{}           `json:"supported_destination_sync_modes"`
}
