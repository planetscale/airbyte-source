package internal

import (
	"context"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"io"
	"os"
)

type PlanetScaleConnection struct {
	Host             string `json:"host"`
	Database         string `json:"database"`
	Username         string `json:"username"`
	Password         string `json:"password"`
	DatabaseAccessor PlanetScaleDatabase
}

func (psc PlanetScaleConnection) DSN() string {
	config := mysql.NewConfig()
	config.Net = "tcp"
	config.Addr = psc.Host
	config.User = psc.Username
	config.DBName = fmt.Sprintf("%v@replica", psc.Database)
	config.Passwd = psc.Password
	if useSecureConnection() {
		config.TLSConfig = "true"
	}
	return config.FormatDSN()
}

func (psc PlanetScaleConnection) Check() error {
	_, err := psc.DatabaseAccessor.CanConnect(context.Background(), psc)
	if err != nil {
		return err
	}
	return nil
}

func (psc PlanetScaleConnection) DiscoverSchema() (c Catalog, err error) {
	return psc.DatabaseAccessor.DiscoverSchema(context.Background(), psc)
}

func (psc PlanetScaleConnection) Read(w io.Writer, table Stream, state string) error {
	return psc.DatabaseAccessor.Read(context.Background(), w, psc, table, state)
}

func useSecureConnection() bool {
	e2eTestRun, found := os.LookupEnv("PS_END_TO_END_TEST_RUN")
	if found && (e2eTestRun == "yes" ||
		e2eTestRun == "y" ||
		e2eTestRun == "true" ||
		e2eTestRun == "1") {
		return false
	}

	return true
}
