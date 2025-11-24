package airbyte_source

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "pscalesource",
	Short: "PlanetScale airbyte source",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Please try one of the sub commands")
	},
}

func Execute(ctx context.Context, ver, commit, buildDate string) {
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
