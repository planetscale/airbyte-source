package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
)

type IPlanetScaleDatabase interface {
	CanConnect(ctx context.Context, ps PlanetScaleConnection) (bool, error)
	DiscoverSchema(ctx context.Context, ps PlanetScaleConnection) (Catalog, error)
	Read(ctx context.Context, ps PlanetScaleConnection, s Stream, state string) error
}

type PlanetScaleMySQLDatabase struct {
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
		return c, err
	}
	defer db.Close()
	tableNamesQR, err := db.Query(fmt.Sprintf("SHOW TABLES FROM `%s`", psc.Keyspace))
	if err != nil {
		return c, err
	}

	var tables []string

	for tableNamesQR.Next() {
		var name string
		if err = tableNamesQR.Scan(&name); err != nil {
			return c, err
		}

		tables = append(tables, name)
	}

	for _, tableName := range tables {
		stream, err := getStreamForTable(tableName, psc.Keyspace, db)
		if err != nil {
			// print something here and move on.
		}
		c.Streams = append(c.Streams, stream)
	}
	return c, nil
}

func getStreamForTable(tableName string, keyspace string, db *sql.DB) (Stream, error) {
	schema := StreamSchema{
		Type:       "object",
		Properties: map[string]PropertyType{},
	}
	stream := Stream{
		Name:               tableName,
		Schema:             schema,
		SupportedSyncModes: []string{"full_refresh"},
		Namespace:          keyspace,
	}

	var columns []string
	query := fmt.Sprintf("select COLUMN_NAME, COLUMN_TYPE from information_schema.columns where table_name=\"%v\" AND TABLE_SCHEMA=\"%v\"", tableName, keyspace)
	columnNamesQR, err := db.Query(query)
	if err != nil {
		return stream, err
	}

	for columnNamesQR.Next() {
		var (
			name       string
			columnType string
		)
		if err = columnNamesQR.Scan(&name, &columnType); err != nil {
			return stream, err
		}

		stream.Schema.Properties[name] = PropertyType{getJsonSchemaType(columnType)}
		columns = append(columns, name)
	}

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

func (p PlanetScaleMySQLDatabase) Read(ctx context.Context, psc PlanetScaleConnection, table Stream, state string) error {
	db, err := sql.Open("mysql", psc.DSN())
	if err != nil {
		log.Printf("Unable to open connection to read stream : %v", err)
		return err
	}
	cursor, err := db.Query(table.GetSelectQuery())
	if err != nil {
		log.Printf("Unable to query rows from table  : %v, query %v failed with %v : ", table.Name, table.GetSelectQuery(), err)
		return err
	}

	cols, _ := cursor.Columns()
	columns := make([]string, len(table.Schema.Properties))
	columnPointers := make([]interface{}, len(table.Schema.Properties))
	for i, _ := range columns {
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

		printRecord(table.Name, m)
	}
	return nil
}

func printRecord(tableName string, record map[string]interface{}) error {
	now := time.Now()
	amsg := AirbyteMessage{
		Type: RECORD,
		Record: &AirbyteRecord{
			Stream:    tableName,
			Data:      record,
			EmittedAt: now.UnixMilli(),
		},
	}

	msg, _ := json.Marshal(amsg)
	fmt.Println(string(msg))
	return nil
}

func printState(state map[string]string) {
	amsg := AirbyteMessage{
		Type:  STATE,
		State: &AirbyteState{state},
	}

	msg, _ := json.Marshal(amsg)
	fmt.Println(string(msg))
}
