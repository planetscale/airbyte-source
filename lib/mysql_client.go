package lib

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
)

type MysqlClient interface {
	BuildSchema(ctx context.Context, psc PlanetScaleSource, schemaBuilder SchemaBuilder) error
	PingContext(context.Context, PlanetScaleSource) error
	GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error)
	Close() error
}

func NewMySQL(psc *PlanetScaleSource) (MysqlClient, error) {
	db, err := sql.Open("mysql", psc.DSN(psdbconnect.TabletType_primary))
	if err != nil {
		return nil, err
	}

	return mysqlClient{
		db: db,
	}, nil
}

type mysqlClient struct {
	db *sql.DB
}

// BuildSchema returns schemas for all tables in a PlanetScale database
// 1. Get all keyspaces for the PlanetScale database
// 2. Get the schemas for all tables in a keyspace, for each keyspace
// 2. Get columns and primary keys for each table from information_schema.columns
// 3. Format results into FiveTran response
func (p mysqlClient) BuildSchema(ctx context.Context, psc PlanetScaleSource, schemaBuilder SchemaBuilder) error {
	keyspaces, err := p.GetKeyspaces(ctx, psc)
	if err != nil {
		return errors.Wrap(err, "Unable to build schema for database")
	}

	for _, keyspaceName := range keyspaces {
		schemaBuilder.OnKeyspace(keyspaceName)
		tableNames, err := p.getKeyspaceTableNames(ctx, keyspaceName)
		if err != nil {
			return errors.Wrap(err, "Unable to build schema for database")
		}

		for _, tableName := range tableNames {
			schemaBuilder.OnTable(keyspaceName, tableName)

			columns, err := p.getKeyspaceTableColumns(ctx, keyspaceName, tableName)
			if err != nil {
				return errors.Wrap(err, "Unable to build schema for database")
			}

			schemaBuilder.OnColumns(keyspaceName, tableName, columns)
		}
	}

	return nil
}

func (p mysqlClient) Close() error {
	return p.db.Close()
}

func (p mysqlClient) getKeyspaceTableColumns(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
	var columns []MysqlColumn
	columnNamesQR, err := p.db.QueryContext(
		ctx,
		"select column_name, column_type, column_key from information_schema.columns where table_name=? AND table_schema=?;",
		tableName, keyspaceName,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to get column names & types for table %v", tableName)
	}
	for columnNamesQR.Next() {
		var (
			name       string
			columnType string
			columnKey  string
		)
		if err = columnNamesQR.Scan(&name, &columnType, &columnKey); err != nil {
			return nil, errors.Wrapf(err, "Unable to scan row for column names & types of table %v", tableName)
		}

		columns = append(columns, MysqlColumn{
			Name:         name,
			Type:         columnType,
			IsPrimaryKey: strings.EqualFold(columnKey, "PRI"),
		})
	}

	if err := columnNamesQR.Err(); err != nil {
		return nil, errors.Wrapf(err, "unable to iterate columns for table %s", tableName)
	}

	return columns, nil
}

func (p mysqlClient) GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
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

func (p mysqlClient) PingContext(ctx context.Context, psc PlanetScaleSource) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return p.db.PingContext(ctx)
}

func (p mysqlClient) getKeyspaceTableNames(ctx context.Context, keyspaceName string) ([]string, error) {
	var tables []string

	tableNamesQR, err := p.db.Query(fmt.Sprintf("show tables from `%s`;", keyspaceName))
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

func (p mysqlClient) GetKeyspaces(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	var keyspaces []string

	// TODO: is there a prepared statement equivalent?
	shardNamesQR, err := p.db.QueryContext(
		ctx,
		`show vitess_keyspaces like "%`+psc.Database+`%";`,
	)
	if err != nil {
		return keyspaces, errors.Wrap(err, "Unable to query database for keyspaces")
	}

	for shardNamesQR.Next() {
		var name string
		if err = shardNamesQR.Scan(&name); err != nil {
			return keyspaces, errors.Wrap(err, "unable to get shard names")
		}

		keyspaces = append(keyspaces, strings.TrimPrefix(name, psc.Database+"/"))
	}

	if err := shardNamesQR.Err(); err != nil {
		return keyspaces, errors.Wrapf(err, "unable to iterate shard names for %s", psc.Database)
	}
	return keyspaces, nil
}
