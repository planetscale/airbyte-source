package airbyte_source

import (
	"encoding/json"
	"io"
	"strings"

	_ "embed"

	"github.com/spf13/cobra"
)

//go:embed spec.json
var staticSpec string

func init() {
	rootCmd.AddCommand(SpecCommand())
}

func minifyJSON(out io.Writer, in io.Reader) error {
	var data any
	if err := json.NewDecoder(in).Decode(&data); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(data)
}

func SpecCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "spec",
		Short: "Describes inputs needed for connecting to PlanetScale databases",
		RunE: func(cmd *cobra.Command, args []string) error {
			// XXX: The spec MUST be output in a single line, so we minify the actual
			// spec before outputting, otherwise Airbyte will not be able to parse it.
			return minifyJSON(cmd.OutOrStdout(), strings.NewReader(staticSpec))
		},
	}
}
