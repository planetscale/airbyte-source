package cmd

import (
	"context"
	"github.com/go-sql-driver/mysql"
)

type PlanetScaleConnection struct {
	Host     string `json:"host"`
	Keyspace string `json:"keyspace"`
	Username string `json:"username"`
	Password string `json"password""`
}

func (psc PlanetScaleConnection) DSN() string {
	config := mysql.NewConfig()
	config.Net = "tcp"
	config.Addr = psc.Host
	config.User = psc.Username
	config.DBName = psc.Keyspace
	config.Passwd = psc.Password
	config.TLSConfig = "true"
	return config.FormatDSN()
}

func (psc PlanetScaleConnection) database() IPlanetScaleDatabase {
	return PlanetScaleVstreamDatabase{}
}

func (psc PlanetScaleConnection) Check() error {
	_, err := psc.database().CanConnect(context.Background(), psc)
	if err != nil {
		return err
	}
	return nil
}

func (psc PlanetScaleConnection) DiscoverSchema() (c Catalog, err error) {
	return psc.database().DiscoverSchema(context.Background(), psc)
}

func (psc PlanetScaleConnection) Read(table Stream, state string) error {
	return psc.database().Read(context.Background(), psc, table, state)
}
