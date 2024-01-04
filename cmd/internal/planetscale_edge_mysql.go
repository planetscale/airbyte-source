package internal

import (
	"context"
	"database/sql"
	"github.com/pkg/errors"
	"strings"
	"time"
)

type VitessTablet struct {
	Cell                 string
	Keyspace             string
	Shard                string
	TabletType           string
	State                string
	Alias                string
	Hostname             string
	PrimaryTermStartTime string
}
type PlanetScaleEdgeMysqlAccess interface {
	PingContext(context.Context, PlanetScaleSource) error
	GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error)
	GetVitessTablets(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error)
	Close() error
}

func NewMySQL(psc *PlanetScaleSource) (PlanetScaleEdgeMysqlAccess, error) {
	db, err := sql.Open("mysql", psc.DSN())
	if err != nil {
		return nil, err
	}

	return planetScaleEdgeMySQLAccess{
		db: db,
	}, nil
}

type planetScaleEdgeMySQLAccess struct {
	db *sql.DB
}

func (p planetScaleEdgeMySQLAccess) Close() error {
	return p.db.Close()
}

func (p planetScaleEdgeMySQLAccess) GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	var shards []string

	// TODO: is there a prepared statement equivalent?
	shardNamesQR, err := p.db.QueryContext(
		ctx,
		`show vitess_shards like "%`+psc.Database+`%";`,
	)
	if err != nil {
		return shards, errors.Wrap(err, "Unable to query database for shards")
	}

	for shardNamesQR.Next() {
		var name string
		if err = shardNamesQR.Scan(&name); err != nil {
			return shards, errors.Wrap(err, "unable to get shard names")
		}

		shards = append(shards, strings.TrimPrefix(name, psc.Database+"/"))
	}

	if err := shardNamesQR.Err(); err != nil {
		return shards, errors.Wrapf(err, "unable to iterate shard names for %s", psc.Database)
	}
	return shards, nil
}

func (p planetScaleEdgeMySQLAccess) GetVitessTablets(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error) {
	var tablets []VitessTablet

	tabletsQR, err := p.db.QueryContext(ctx, "Show vitess_tablets")
	if err != nil {
		return tablets, err
	}

	for tabletsQR.Next() {
		vt := VitessTablet{}
		// output is of the form :
		//aws_useast1c_5 connect-test - PRIMARY SERVING aws_useast1c_5-2797914161 10.200.131.217 2022-05-09T14:11:56Z
		//aws_useast1c_5 connect-test - REPLICA SERVING aws_useast1c_5-1559247072 10.200.178.136
		//aws_useast1c_5 connect-test - PRIMARY SERVING aws_useast1c_5-2797914161 10.200.131.217 2022-05-09T14:11:56Z
		//aws_useast1c_5 connect-test - REPLICA SERVING aws_useast1c_5-1559247072 10.200.178.136
		err := tabletsQR.Scan(&vt.Cell, &vt.Keyspace, &vt.Shard, &vt.TabletType, &vt.State, &vt.Alias, &vt.Hostname, &vt.PrimaryTermStartTime)
		if err != nil {
			return tablets, err
		}
		tablets = append(tablets, vt)
	}
	if err := tabletsQR.Err(); err != nil {
		return tablets, errors.Wrapf(err, "unable to iterate tablets for %s", psc.Database)
	}
	return tablets, nil
}

func (p planetScaleEdgeMySQLAccess) PingContext(ctx context.Context, psc PlanetScaleSource) error {

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return p.db.PingContext(ctx)
}
