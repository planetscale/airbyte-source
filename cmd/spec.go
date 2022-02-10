package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(specCmd)
}

const rawspec = `{"type":"SPEC","spec":{"documentationUrl":"https://docs.airbyte.io/integrations/sources/mysql","connectionSpecification":{"$schema":"http://json-schema.org/draft-07/schema#","title":"PlanetScale Source Spec","type":"object","required":["host","database","username","replication_method"],"additionalProperties":false,"properties":{"host":{"description":"The host name of the database.","title":"Host","type":"string","order":0},"database":{"description":"The PlanetScale database name.","title":"Database","type":"string","order":1},"username":{"description":"The username which is used to access the database.","title":"Username","type":"string","order":2},"password":{"description":"The password associated with the username.","title":"Password","type":"string","airbyte_secret":true,"order":3},"replication_method":{"type":"string","title":"Replication Method","description":"Replication method which is used for data extraction from the database. STANDARD replication requires no setup on the DB side but will not be able to represent deletions incrementally. CDC uses the Binlog to detect inserts, updates, and deletes. This needs to be configured on the source database itself.","order":4,"default":"STANDARD","enum":["STANDARD","PlanetScale"]}}},"supportsNormalization":false,"supportsDBT":false,"supported_destination_sync_modes":[]}}`

var specCmd = &cobra.Command{
	Use:   "spec",
	Short: "Describes inputs needed for connecting to PlanetScale databases",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(rawspec)
	},
}
