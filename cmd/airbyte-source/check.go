package airbyte_source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/planetscale/connect/source/cmd/internal"
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

			psc, err := parseSource(ch.FileReader, configFilePath)
			if err != nil {
				cs := internal.ConnectionStatus{
					Status:  "FAILED",
					Message: fmt.Sprintf("Configuration for PlanetScale database is invalid, unable to read source configuration : %v", err),
				}
				ch.Logger.ConnectionStatus(cs)
				return
			}

			if err := ch.EnsureDB(psc); err != nil {
				fmt.Fprintln(cmd.OutOrStdout(), "Unable to connect to PlanetScale Database")
				return
			}

			defer func() {
				if err := ch.Database.Close(); err != nil {
					fmt.Fprintf(cmd.OutOrStdout(), "Unable to close connection to PlanetScale Database, failed with %v", err)
				}
			}()

			cs, _ := checkConnectionStatus(ch.Database, psc)
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

func parseSource(reader FileReader, configFilePath string) (internal.PlanetScaleSource, error) {
	var psc internal.PlanetScaleSource
	contents, err := reader.ReadFile(configFilePath)
	if err != nil {
		return psc, err
	}
	if err = json.Unmarshal(contents, &psc); err != nil {
		return psc, err
	}

	return psc, nil
}

func checkConnectionStatus(database internal.PlanetScaleDatabase, psc internal.PlanetScaleSource) (internal.ConnectionStatus, error) {

	if err := database.CanConnect(context.Background(), psc); err != nil {
		return internal.ConnectionStatus{
			Status:  "FAILED",
			Message: fmt.Sprintf("Unable to connect to PlanetScale database %v at host %v with username %v. Failed with \n %v", psc.Database, psc.Host, psc.Username, err),
		}, err
	}

	return internal.ConnectionStatus{
		Status:  "SUCCEEDED",
		Message: fmt.Sprintf("Successfully connected to database %v at host %v with username %v", psc.Database, psc.Host, psc.Username),
	}, nil
}
