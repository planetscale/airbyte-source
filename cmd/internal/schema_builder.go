package internal

import (
	"github.com/planetscale/connectsdk/lib"
	"regexp"
	"strings"
)

const (
	gCTableNameExpression string = `^_vt_(HOLD|PURGE|EVAC|DROP)_([0-f]{32})_([0-9]{14})$`
)

var gcTableNameRegexp = regexp.MustCompile(gCTableNameExpression)

type SchemaBuilder struct {
	catalog *Catalog
	streams map[string]map[string]*Stream
}

func NewSchemaBuilder() lib.SchemaBuilder {
	return &SchemaBuilder{}
}

func (sb *SchemaBuilder) OnKeyspace(_ string) {
	// no-op as Airbyte has schemas as a flat list.
}

func (sb *SchemaBuilder) OnTable(keyspaceName, tableName string) {
	// skip any that are Vitess's GC tables.
	if gcTableNameRegexp.MatchString(tableName) {
		return
	}

	schema := StreamSchema{
		Type:       "object",
		Properties: map[string]PropertyType{},
	}

	stream := &Stream{
		Name:               tableName,
		Schema:             schema,
		SupportedSyncModes: []string{"full_refresh", "incremental"},
		Namespace:          keyspaceName,
	}

	if sb.streams == nil {
		sb.streams = make(map[string]map[string]*Stream)
	}

	if _, ok := sb.streams[keyspaceName]; !ok {
		sb.streams[keyspaceName] = make(map[string]*Stream)
	}

	sb.streams[keyspaceName][tableName] = stream
}

func (sb *SchemaBuilder) OnColumns(keyspaceName, tableName string, columns []lib.MysqlColumn) {
	if _, ok := sb.streams[keyspaceName]; !ok {
		return
	}

	if _, ok := sb.streams[keyspaceName][tableName]; !ok {
		return
	}

	table := sb.streams[keyspaceName][tableName]
	table.PrimaryKeys = [][]string{}
	table.DefaultCursorFields = []string{}

	for _, column := range columns {
		if column.IsPrimaryKey {
			table.PrimaryKeys = append(table.PrimaryKeys, []string{column.Name})
			table.DefaultCursorFields = append(table.DefaultCursorFields, column.Name)
		}

		table.Schema.Properties[column.Name] = getAirbyteDataType(column.Type, true)
	}
}

func (sb *SchemaBuilder) GetCatalog() Catalog {
	c := Catalog{}
	for _, keyspace := range sb.streams {
		for _, table := range keyspace {
			c.Streams = append(c.Streams, *table)
		}
	}
	return c
}

// Convert columnType to Airbyte type.
func getAirbyteDataType(mysqlType string, treatTinyIntAsBoolean bool) PropertyType {
	// Support custom airbyte types documented here :
	// https://docs.airbyte.com/understanding-airbyte/supported-data-types/#the-types
	switch {
	case strings.HasPrefix(mysqlType, "tinyint(1)"):
		if treatTinyIntAsBoolean {
			return PropertyType{Type: "boolean"}
		}
		return PropertyType{Type: "number", AirbyteType: "integer"}
	case strings.HasPrefix(mysqlType, "int"), strings.HasPrefix(mysqlType, "smallint"), strings.HasPrefix(mysqlType, "mediumint"), strings.HasPrefix(mysqlType, "bigint"), strings.HasPrefix(mysqlType, "tinyint"):
		return PropertyType{Type: "number", AirbyteType: "integer"}
	case strings.HasPrefix(mysqlType, "decimal"), strings.HasPrefix(mysqlType, "double"), strings.HasPrefix(mysqlType, "float"):
		return PropertyType{Type: "number"}
	case strings.HasPrefix(mysqlType, "datetime"), strings.HasPrefix(mysqlType, "timestamp"):
		return PropertyType{Type: "string", CustomFormat: "date-time", AirbyteType: "timestamp_without_timezone"}
	case strings.HasPrefix(mysqlType, "date"):
		return PropertyType{Type: "string", CustomFormat: "date", AirbyteType: "date"}
	case strings.HasPrefix(mysqlType, "time"):
		return PropertyType{Type: "string", CustomFormat: "time", AirbyteType: "time_without_timezone"}
	default:
		return PropertyType{Type: "string"}
	}
}
