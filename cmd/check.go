package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
)

var configFilePath string

func init() {
	checkCmd.Flags().StringVar(&configFilePath, "config", "", "Path to the PlanetScale source configuration")
	rootCmd.AddCommand(checkCmd)
}

var checkCmd = &cobra.Command{
	Use:   "check",
	Short: "Validates the credentials to connect to a PlanetScale database",
	Run: func(cmd *cobra.Command, args []string) {
		cs, _, err := checkConfig(configFilePath)
		if err != nil {
			printConnectionStatus(cs, "Connection test failed", LOGLEVEL_ERROR)
			os.Exit(1)
		} else {
			printConnectionStatus(cs, "Connection test succeeded", LOGLEVEL_INFO)
		}
	},
}

func printConnectionStatus(status ConnectionStatus, message, level string) {
	amsg := AirbyteMessage{
		Type:             CONNECTION_STATUS,
		ConnectionStatus: &status,
		Log: &AirbyteLogMessage{
			Level:   level,
			Message: message,
		},
	}
	msg, _ := json.Marshal(amsg)
	fmt.Println(string(msg))
}

func checkConfig(configFilePath string) (ConnectionStatus, PlanetScaleConnection, error) {
	var psc PlanetScaleConnection
	b, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		return ConnectionStatus{
			Status:  "FAILED",
			Message: fmt.Sprintf("Configuration for PlanetScale database is invalid, unable to read source configuration : %v", err),
		}, psc, err

	}

	if err = json.Unmarshal(b, &psc); err != nil {
		return ConnectionStatus{
			Status:  "FAILED",
			Message: fmt.Sprintf("Configuration for PlanetScale database is invalid, unable to parse source JSON: %v", err),
		}, psc, err

	}

	if err = psc.Check(); err != nil {
		return ConnectionStatus{
			Status:  "FAILED",
			Message: fmt.Sprintf("Unable to connect to PlanetScale database %v at host %v with username %v. Failed with \n %v", psc.Database, psc.Host, psc.Username, err),
		}, psc, err
	}

	return ConnectionStatus{
		Status:  "SUCCEEDED",
		Message: fmt.Sprintf("Successfully connected to database keyspace %v at host %v with username %v", psc.Database, psc.Host, psc.Username),
	}, psc, nil

}
