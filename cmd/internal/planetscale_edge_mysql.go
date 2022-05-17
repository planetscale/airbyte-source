package internal

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/edge-gateway/proto/psdbconnect/v1alpha1"
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
	PingContext(context.Context, PlanetScaleSource) (bool, error)
	GetTableNames(context.Context, PlanetScaleSource) ([]string, error)
	GetTableSchema(context.Context, PlanetScaleSource, string) (map[string]PropertyType, error)
	GetTablePrimaryKeys(context.Context, PlanetScaleSource, string) ([]string, error)
	QueryContext(ctx context.Context, psc PlanetScaleSource, query string, args ...interface{}) (*sql.Rows, error)
	GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error)
	GetVitessTablets(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error)
}

func NewMySQL() PlanetScaleEdgeMysqlAccess {
	return planetScaleEdgeMySQLAccess{}
}

func (a planetScaleEdgeMySQLAccess) GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	var shards []string
	var db *sql.DB
	db, err := sql.Open("mysql", psc.DSN(psdbconnect.TabletType_primary))
	if err != nil {
		return shards, err
	}
	defer db.Close()
	// TODO: is there a prepared statement equivalent?
	shardNamesQR, err := db.QueryContext(
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

func (a planetScaleEdgeMySQLAccess) GetVitessTablets(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error) {
	var tablets []VitessTablet
	var db *sql.DB
	db, err := sql.Open("mysql", psc.DSN(psdbconnect.TabletType_primary))
	if err != nil {
		return tablets, err
	}
	defer db.Close()
	tabletsQR, err := db.QueryContext(ctx, "Show vitess_tablets")
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
	return tablets, nil
}

type planetScaleEdgeMySQLAccess struct{}

func (planetScaleEdgeMySQLAccess) PingContext(ctx context.Context, psc PlanetScaleSource) (bool, error) {
	var db *sql.DB
	db, err := sql.Open("mysql", psc.DSN(psdbconnect.TabletType_primary))
	if err != nil {
		return false, err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (a planetScaleEdgeMySQLAccess) GetTableNames(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	var tables []string
	db, err := sql.Open("mysql", psc.DSN(psdbconnect.TabletType_primary))
	if err != nil {
		return tables, errors.Wrap(err, "Unable to open SQL connection")
	}
	defer db.Close()

	tableNamesQR, err := db.Query(fmt.Sprintf("show tables from `%s`;", psc.Database))
	if err != nil {
		return tables, errors.Wrap(err, "Unable to query database for schema")
	}

	for tableNamesQR.Next() {
		var name string
		if err = tableNamesQR.Scan(&name); err != nil {
			return tables, errors.Wrap(err, "unable to get table names")
		}

		tables = append(tables, name)
	}
	if err := tableNamesQR.Err(); err != nil {
		return tables, errors.Wrap(err, "unable to iterate table rows")
	}
	return tables, err
}

func (a planetScaleEdgeMySQLAccess) GetTableSchema(ctx context.Context, psc PlanetScaleSource, tableName string) (map[string]PropertyType, error) {
	properties := map[string]PropertyType{}
	db, err := sql.Open("mysql", psc.DSN(psdbconnect.TabletType_primary))
	if err != nil {
		return properties, errors.Wrap(err, "Unable to open SQL connection")
	}
	defer db.Close()

	columnNamesQR, err := db.QueryContext(
		ctx,
		"select column_name, column_type from information_schema.columns where table_name=? AND table_schema=?;",
		tableName, psc.Database,
	)
	if err != nil {
		return properties, errors.Wrapf(err, "Unable to get column names & types for table %v", tableName)
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

func (a planetScaleEdgeMySQLAccess) GetTablePrimaryKeys(ctx context.Context, psc PlanetScaleSource, tableName string) ([]string, error) {
	var primaryKeys []string
	db, err := sql.Open("mysql", psc.DSN(psdbconnect.TabletType_primary))
	if err != nil {
		return primaryKeys, errors.Wrap(err, "Unable to open SQL connection")
	}
	defer db.Close()
	primaryKeysQR, err := db.QueryContext(
		ctx,
		"select column_name from information_schema.columns where table_schema=? AND table_name=? AND column_key='PRI';",
		psc.Database, tableName,
	)
	
	if err != nil {
		return primaryKeys, errors.Wrapf(err, "Unable to scan row for primary keys of table %v", tableName)
	}

	for primaryKeysQR.Next() {
		var name string
		if err = primaryKeysQR.Scan(&name); err != nil {
			return primaryKeys, errors.Wrapf(err, "Unable to scan row for primary keys of table %v", tableName)
		}

		primaryKeys = append(primaryKeys, name)
	}

	if err := primaryKeysQR.Err(); err != nil {
		return primaryKeys, errors.Wrapf(err, "unable to iterate primary keys for table %s", tableName)
	}
	return primaryKeys, nil
}

func (planetScaleEdgeMySQLAccess) QueryContext(ctx context.Context, psc PlanetScaleSource, query string, args ...interface{}) (*sql.Rows, error) {
	//TODO implement me
	panic("implement me")
}
