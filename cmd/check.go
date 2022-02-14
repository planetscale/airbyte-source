package cmd

import (
	"encoding/json"
	"fmt"
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

func printConnectionStatus(writer io.Writer, status ConnectionStatus, message, level string) {
	amsg := AirbyteMessage{
		Type:             CONNECTION_STATUS,
		ConnectionStatus: &status,
		Log: &AirbyteLogMessage{
			Level:   level,
			Message: message,
		},
	}
	msg, _ := json.Marshal(amsg)
	fmt.Fprintf(writer, "%s\n", string(msg))
}

func checkConnectionStatus(database PlanetScaleDatabase, reader FileReader, configFilePath string) (ConnectionStatus, PlanetScaleConnection, error) {
	var psc PlanetScaleConnection
	contents, err := reader.ReadFile(configFilePath)
	if err != nil {
		return ConnectionStatus{
			Status:  "FAILED",
			Message: fmt.Sprintf("Configuration for PlanetScale database is invalid, unable to read source configuration : %v", err),
		}, psc, err
	}

	if err = json.Unmarshal(contents, &psc); err != nil {
		return ConnectionStatus{
			Status:  "FAILED",
			Message: fmt.Sprintf("Configuration for PlanetScale database is invalid, unable to parse source JSON: %v", err),
		}, psc, err
	}

	psc.database = database
	if err = psc.Check(); err != nil {
		return ConnectionStatus{
			Status:  "FAILED",
			Message: fmt.Sprintf("Unable to connect to PlanetScale database %v at host %v with username %v. Failed with \n %v", psc.Database, psc.Host, psc.Username, err),
		}, psc, err
	}

	return ConnectionStatus{
		Status:  "SUCCEEDED",
		Message: fmt.Sprintf("Successfully connected to database %v at host %v with username %v", psc.Database, psc.Host, psc.Username),
	}, psc, nil
}
