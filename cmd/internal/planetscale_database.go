package internal

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	psdbdatav1 "github.com/planetscale/edge-gateway/proto/psdb/data_v1"
	"io"
	"log"
	"strings"
	"time"
)

type PlanetScaleDatabase interface {
	CanConnect(ctx context.Context, ps PlanetScaleConnection) (bool, error)
	DiscoverSchema(ctx context.Context, ps PlanetScaleConnection) (Catalog, error)
	ListShards(ctx context.Context, ps PlanetScaleConnection) ([]string, error)
	Read(ctx context.Context, w io.Writer, ps PlanetScaleConnection, s ConfiguredStream, maxReadDuration time.Duration, tc *psdbdatav1.TableCursor) (*SerializedCursor, error)
}

type PlanetScaleMySQLDatabase struct {
	Logger AirbyteLogger
}

func (p PlanetScaleMySQLDatabase) CanConnect(ctx context.Context, psc PlanetScaleConnection) (bool, error) {
	var db *sql.DB
	db, err := sql.Open("mysql", psc.DSN())
	if err != nil {
		return false, err
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		return false, err
	}

	return true, nil
}

func (p PlanetScaleMySQLDatabase) DiscoverSchema(ctx context.Context, psc PlanetScaleConnection) (c Catalog, err error) {
	var db *sql.DB

	db, err = sql.Open("mysql", psc.DSN())
	if err != nil {
		return c, errors.Wrap(err, "Unable to open SQL connection")
	}
	defer db.Close()
	tableNamesQR, err := db.Query(fmt.Sprintf("SHOW TABLES FROM `%s`", psc.Database))
	if err != nil {
		return c, errors.Wrap(err, "Unable to query database for schema")
	}

	var tables []string

	for tableNamesQR.Next() {
		var name string
		if err = tableNamesQR.Scan(&name); err != nil {
			return c, errors.Wrap(err, "unable to get table names")
		}

		tables = append(tables, name)
	}

	for _, tableName := range tables {
		stream, err := getStreamForTable(tableName, psc.Database, db)
		if err != nil {
			return c, errors.Wrapf(err, "unable to get stream for table %v", tableName)
		}
		c.Streams = append(c.Streams, stream)
	}
	return c, nil
}

func (p PlanetScaleMySQLDatabase) ListShards(ctx context.Context, psc PlanetScaleConnection) ([]string, error) {
	var shards []string

	db, err := sql.Open("mysql", psc.DSN())
	if err != nil {
		return shards, errors.Wrap(err, "Unable to open SQL connection")
	}
	defer db.Close()
	shardNamesQR, err := db.Query("show vitess_shards like \"%" + psc.Database + "%\"")
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
	return shards, nil
}
func getStreamForTable(tableName string, keyspace string, db *sql.DB) (Stream, error) {
	schema := StreamSchema{
		Type:       "object",
		Properties: map[string]PropertyType{},
	}
	stream := Stream{
		Name:               tableName,
		Schema:             schema,
		SupportedSyncModes: []string{"full_refresh", "incremental"},
		Namespace:          keyspace,
	}

	query := fmt.Sprintf("select COLUMN_NAME, COLUMN_TYPE from information_schema.columns where table_name=\"%v\" AND TABLE_SCHEMA=\"%v\"", tableName, keyspace)
	columnNamesQR, err := db.Query(query)
	if err != nil {
		return stream, errors.Wrapf(err, "Unable to get column names & types for table %v", tableName)
	}

	for columnNamesQR.Next() {
		var (
			name       string
			columnType string
		)
		if err = columnNamesQR.Scan(&name, &columnType); err != nil {
			return stream, errors.Wrapf(err, "Unable to scan row for column names & types of table %v", tableName)
		}

		stream.Schema.Properties[name] = PropertyType{getJsonSchemaType(columnType)}
	}

	primaryKeysQuery := fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%v'   AND TABLE_NAME = '%v'   AND COLUMN_KEY = 'PRI';", keyspace, tableName)
	primaryKeysQR, err := db.Query(primaryKeysQuery)
	if err != nil {
		return stream, errors.Wrapf(err, "Unable to get primary key column names for table %v", tableName)
	}

	for primaryKeysQR.Next() {
		var (
			name string
		)
		if err = primaryKeysQR.Scan(&name); err != nil {
			return stream, errors.Wrapf(err, "Unable to scan row for primary keys of table %v", tableName)
		}

		stream.PrimaryKeys = append(stream.PrimaryKeys, []string{name})
		stream.DefaultCursorFields = append(stream.DefaultCursorFields, name)
	}
	stream.SourceDefinedCursor = true
	return stream, nil
}

// Convert columnType to Airbyte type.
func getJsonSchemaType(mysqlType string) string {
	if strings.HasPrefix(mysqlType, "int") {
		return "integer"
	}

	if mysqlType == "tinyint(1)" {
		return "boolean"
	}

	return "string"
}

func (p PlanetScaleMySQLDatabase) Read(ctx context.Context, w io.Writer, psc PlanetScaleConnection, s ConfiguredStream, maxReadDuration time.Duration, tc *psdbdatav1.TableCursor) (*SerializedCursor, error) {
	var sc *SerializedCursor
	table := s.Stream
	db, err := sql.Open("mysql", psc.DSN())

	if err != nil {
		log.Printf("Unable to open connection to read stream : %v", err)
		return sc, err
	}
	cursor, err := db.Query(table.GetSelectQuery())
	if err != nil {
		log.Printf("Unable to query rows from table  : %v, query %v failed with %v : ", table.Name, table.GetSelectQuery(), err)
		return sc, err
	}

	cols, _ := cursor.Columns()
	columns := make([]string, len(table.Schema.Properties))
	columnPointers := make([]interface{}, len(table.Schema.Properties))
	for i := range columns {
		columnPointers[i] = &columns[i]
	}
	for cursor.Next() {
		err := cursor.Scan(columnPointers...)
		if err != nil {
			// handle err
			log.Fatal(err)
		}
		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make(map[string]interface{})
		for i, colName := range cols {
			val := columnPointers[i].(*string)
			m[colName] = *val
		}

		p.Logger.Record(psc.Database, table.Name, m)
	}
	return sc, nil
}
