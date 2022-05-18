package airbyte_source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/planetscale/airbyte-source/cmd/internal"
	"github.com/spf13/cobra"
)

var configFilePath string

func init() {
	rootCmd.AddCommand(CheckCommand(DefaultHelper(os.Stdout)))
}

func CheckCommand(ch *Helper) *cobra.Command {
	checkCmd := &cobra.Command{
		Use:   "check",
		Short: "Validates the credentials to connect to a PlanetScale database",
		Run: func(cmd *cobra.Command, args []string) {
			ch.Logger = internal.NewLogger(cmd.OutOrStdout())
			if configFilePath == "" {
				fmt.Fprintln(cmd.OutOrStdout(), "Please provide path to a valid configuration file")
				return
			}

			cs, _, _ := checkConnectionStatus(ch.Database, ch.FileReader, configFilePath)
			ch.Logger.ConnectionStatus(cs)
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

func checkConnectionStatus(database internal.PlanetScaleDatabase, reader FileReader, configFilePath string) (internal.ConnectionStatus, internal.PlanetScaleSource, error) {
	var psc internal.PlanetScaleSource
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

	if err := database.CanConnect(context.Background(), psc); err != nil {
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
