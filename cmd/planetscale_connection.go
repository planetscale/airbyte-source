package cmd

import (
	"context"
	"github.com/go-sql-driver/mysql"
	"io"
)

type PlanetScaleConnection struct {
	Host     string `json:"host"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
	database PlanetScaleDatabase
}

func (psc PlanetScaleConnection) DSN() string {
	config := mysql.NewConfig()
	config.Net = "tcp"
	config.Addr = psc.Host
	config.User = psc.Username
	config.DBName = psc.Database
	config.Passwd = psc.Password
	config.TLSConfig = "true"
	return config.FormatDSN()
}

func (psc PlanetScaleConnection) Check() error {
	_, err := psc.database.CanConnect(context.Background(), psc)
	if err != nil {
		return err
	}
	return nil
}

func (psc PlanetScaleConnection) DiscoverSchema() (c Catalog, err error) {
	return psc.database.DiscoverSchema(context.Background(), psc)
}

func (psc PlanetScaleConnection) Read(w io.Writer, table Stream, state string) error {
	return psc.database.Read(context.Background(), w, psc, table, state)
}
