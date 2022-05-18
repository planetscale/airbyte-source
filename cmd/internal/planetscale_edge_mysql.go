package internal

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
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
	GetTableNames(context.Context, PlanetScaleSource) ([]string, error)
	GetTableSchema(context.Context, PlanetScaleSource, string) (map[string]PropertyType, error)
	GetTablePrimaryKeys(context.Context, PlanetScaleSource, string) ([]string, error)
	GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error)
	GetVitessTablets(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error)
	Close() error
}

func NewMySQL() PlanetScaleEdgeMysqlAccess {
	return planetScaleEdgeMySQLAccess{}
}

type planetScaleEdgeMySQLAccess struct {
	psc *PlanetScaleSource
	db  *sql.DB
}

func (p *planetScaleEdgeMySQLAccess) ensureDB(psc *PlanetScaleSource) error {
	if p.db != nil && p.psc == psc {
		return nil
	}

	var err error

	p.db, err = sql.Open("mysql", psc.DSN(psdbconnect.TabletType_primary))
	if err != nil {
		return err
	}

	p.psc = psc
	return nil
}

func (p planetScaleEdgeMySQLAccess) Close() error {
	return p.db.Close()
}

func (p planetScaleEdgeMySQLAccess) GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	var shards []string
	err := p.ensureDB(&psc)
	if err != nil {
		return shards, err
	}
	// TODO: is there a prepared statement equivalent?
	shardNamesQR, err := p.db.QueryContext(
		ctx,
		`show vitess_shards like "%`+psc.Database+`%";`,
	)
	if err != nil {
		return shards, errors.Wrap(err, "Unable to query database for shards")
	}

	if err := shardNamesQR.Err(); err != nil {
		return shards, errors.Wrapf(err, "unable to iterate shard names for %s", psc.Database)
	}

	for shardNamesQR.Next() {
		var name string
		if err = shardNamesQR.Scan(&name); err != nil {
			return shards, errors.Wrap(err, "unable to get shard names")
		}

		shards = append(shards, strings.TrimPrefix(name, psc.Database+"/"))
	}
	return shards, nil
}

func (p planetScaleEdgeMySQLAccess) GetVitessTablets(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error) {
	var tablets []VitessTablet
	err := p.ensureDB(&psc)
	if err != nil {
		return tablets, err
	}

	tabletsQR, err := p.db.QueryContext(ctx, "Show vitess_tablets")
	if err != nil {
		return tablets, err
	}
	if err := tabletsQR.Err(); err != nil {
		return tablets, errors.Wrapf(err, "unable to iterate tablets for %s", psc.Database)
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
	return tablets, nil
}

func (p planetScaleEdgeMySQLAccess) PingContext(ctx context.Context, psc PlanetScaleSource) error {
	err := p.ensureDB(&psc)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = p.db.PingContext(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (p planetScaleEdgeMySQLAccess) GetTableNames(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	var tables []string
	err := p.ensureDB(&psc)
	if err != nil {
		return tables, err
	}

	tableNamesQR, err := p.db.Query(fmt.Sprintf("show tables from `%s`;", psc.Database))
	if err != nil {
		return tables, errors.Wrap(err, "Unable to query database for schema")
	}

	if err := tableNamesQR.Err(); err != nil {
		return tables, errors.Wrap(err, "unable to iterate table rows")
	}

	for tableNamesQR.Next() {
		var name string
		if err = tableNamesQR.Scan(&name); err != nil {
			return tables, errors.Wrap(err, "unable to get table names")
		}

		tables = append(tables, name)
	}

	return tables, err
}

func (p planetScaleEdgeMySQLAccess) GetTableSchema(ctx context.Context, psc PlanetScaleSource, tableName string) (map[string]PropertyType, error) {
	properties := map[string]PropertyType{}
	err := p.ensureDB(&psc)
	if err != nil {
		return properties, err
	}

	columnNamesQR, err := p.db.QueryContext(
		ctx,
		"select column_name, column_type from information_schema.columns where table_name=? AND table_schema=?;",
		tableName, psc.Database,
	)
	if err != nil {
		return properties, errors.Wrapf(err, "Unable to get column names & types for table %v", tableName)
	}

	if err := columnNamesQR.Err(); err != nil {
		return properties, errors.Wrapf(err, "unable to iterate columns for table %s", tableName)
	}

	for columnNamesQR.Next() {
		var (
			name       string
			columnType string
		)
		if err = columnNamesQR.Scan(&name, &columnType); err != nil {
			return properties, errors.Wrapf(err, "Unable to scan row for column names & types of table %v", tableName)
		}

		properties[name] = PropertyType{getJsonSchemaType(columnType)}
	}
	return properties, nil
}

func (p planetScaleEdgeMySQLAccess) GetTablePrimaryKeys(ctx context.Context, psc PlanetScaleSource, tableName string) ([]string, error) {
	var primaryKeys []string
	err := p.ensureDB(&psc)
	if err != nil {
		return primaryKeys, err
	}

	primaryKeysQR, err := p.db.QueryContext(
		ctx,
		"select column_name from information_schema.columns where table_schema=? AND table_name=? AND column_key='PRI';",
		psc.Database, tableName,
	)

	if err != nil {
		return primaryKeys, errors.Wrapf(err, "Unable to scan row for primary keys of table %v", tableName)
	}

	if err := primaryKeysQR.Err(); err != nil {
		return primaryKeys, errors.Wrapf(err, "unable to iterate primary keys for table %s", tableName)
	}

	for primaryKeysQR.Next() {
		var name string
		if err = primaryKeysQR.Scan(&name); err != nil {
			return primaryKeys, errors.Wrapf(err, "Unable to scan row for primary keys of table %v", tableName)
		}

		primaryKeys = append(primaryKeys, name)
	}

	return primaryKeys, nil
}
