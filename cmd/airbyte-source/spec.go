package airbyte_source

import (
	"fmt"

	_ "embed"
	"github.com/spf13/cobra"
)

//go:embed spec.json
var staticSpec string

func init() {
	rootCmd.AddCommand(SpecCommand())
}

func SpecCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "spec",
		Short: "Describes inputs needed for connecting to PlanetScale databases",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", staticSpec)
		},
	}
}
