package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(SpecCommand())
}

const rawspec = `{"type":"SPEC","spec":{"documentationUrl":"https://docs.airbyte.io/integrations/sources/mysql","connectionSpecification":{"$schema":"http://json-schema.org/draft-07/schema#","title":"PlanetScale Source Spec","type":"object","required":["host","database","username", "password"],"additionalProperties":false,"properties":{"host":{"description":"The host name of the database.","title":"Host","type":"string","order":0},"database":{"description":"The PlanetScale database name.","title":"Database","type":"string","order":1},"username":{"description":"The username which is used to access the database.","title":"Username","type":"string","order":2},"password":{"description":"The password associated with the username.","title":"Password","type":"string","airbyte_secret":true,"order":3},"replication_method":{"type":"string","title":"Replication Method","description":"Replication method which is used for data extraction from the database. STANDARD replication requires no setup on the DB side but will not be able to represent deletions incrementally. CDC uses the Binlog to detect inserts, updates, and deletes. This needs to be configured on the source database itself.","order":4,"default":"STANDARD","enum":["STANDARD","PlanetScale"]}}},"supportsNormalization":false,"supportsDBT":false,"supported_destination_sync_modes":[]}}`

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
			fmt.Fprintf(cmd.OutOrStdout(), string(msg))
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
