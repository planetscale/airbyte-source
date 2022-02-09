package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "pscalesource",
	Short: "PlanetScale airbyte source",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Please try one of the sub commands")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
