package airbyte_source

import (
	"encoding/json"
	"fmt"
	"github.com/planetscale/connect/source/cmd/internal"
	"github.com/spf13/cobra"
	"io"
)

var configFilePath string

func init() {
	rootCmd.AddCommand(CheckCommand(DefaultHelper()))
}

func CheckCommand(ch *Helper) *cobra.Command {
	checkCmd := &cobra.Command{
		Use:   "check",
		Short: "Validates the credentials to connect to a PlanetScale database",
		Run: func(cmd *cobra.Command, args []string) {
			if configFilePath == "" {
				fmt.Fprintln(cmd.OutOrStdout(), "Please provide path to a valid configuration file")
				return
			}

			cs, _, _ := checkConnectionStatus(ch.Database, ch.FileReader, configFilePath)
			ch.Logger.ConnectionStatus(cmd.OutOrStdout(), cs)
		},
	}
	checkCmd.Flags().StringVar(&configFilePath, "config", "", "Path to the PlanetScale source configuration")
	return checkCmd
}

func printConnectionStatus(writer io.Writer, status internal.ConnectionStatus, message, level string) {
	amsg := internal.AirbyteMessage{
		Type:             internal.CONNECTION_STATUS,
		ConnectionStatus: &status,
		Log: &internal.AirbyteLogMessage{
			Level:   level,
			Message: message,
		},
	}
	msg, _ := json.Marshal(amsg)
	fmt.Fprintf(writer, "%s\n", string(msg))
}

func checkConnectionStatus(database internal.PlanetScaleDatabase, reader FileReader, configFilePath string) (internal.ConnectionStatus, internal.PlanetScaleConnection, error) {
	var psc internal.PlanetScaleConnection
	contents, err := reader.ReadFile(configFilePath)
	if err != nil {
		return internal.ConnectionStatus{
			Status:  "FAILED",
			Message: fmt.Sprintf("Configuration for PlanetScale database is invalid, unable to read source configuration : %v", err),
		}, psc, err
	}

	if err = json.Unmarshal(contents, &psc); err != nil {
		return internal.ConnectionStatus{
			Status:  "FAILED",
			Message: fmt.Sprintf("Configuration for PlanetScale database is invalid, unable to parse source JSON: %v", err),
		}, psc, err
	}

	psc.DatabaseAccessor = database
	if err = psc.Check(); err != nil {
		return internal.ConnectionStatus{
			Status:  "FAILED",
			Message: fmt.Sprintf("Unable to connect to PlanetScale database %v at host %v with username %v. Failed with \n %v", psc.Database, psc.Host, psc.Username, err),
		}, psc, err
	}

	return internal.ConnectionStatus{
		Status:  "SUCCEEDED",
		Message: fmt.Sprintf("Successfully connected to database %v at host %v with username %v", psc.Database, psc.Host, psc.Username),
	}, psc, nil
}