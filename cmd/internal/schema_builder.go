package internal

import (
	"strings"

	"github.com/planetscale/airbyte-source/lib"
)

type AirbyteSchemaBuilder struct {
	catalog               *Catalog
	tables                map[string]map[string]*Stream
	treatTinyIntAsBoolean bool
}

func NewSchemaBuilder(treatTinyIntAsBoolean bool) lib.SchemaBuilder {
	return &AirbyteSchemaBuilder{
		treatTinyIntAsBoolean: treatTinyIntAsBoolean,
	}
}

func (s *AirbyteSchemaBuilder) OnKeyspace(keyspaceName string) {
	if s.catalog == nil {
		s.catalog = &Catalog{
			Streams: []*Stream{},
		}
	}
	if s.tables == nil {
		s.tables = map[string]map[string]*Stream{}
	}
	_, ok := s.tables[keyspaceName]
	if !ok {
		s.tables[keyspaceName] = map[string]*Stream{}
	}
}

func (s *AirbyteSchemaBuilder) OnTable(keyspaceName, tableName string) {
	keyspace, ok := s.tables[keyspaceName]
	if !ok {
		s.OnKeyspace(keyspaceName)
		keyspace = s.tables[keyspaceName]
	}

	if _, ok = keyspace[tableName]; !ok {

		schema := StreamSchema{
			Type:       "object",
			Properties: map[string]PropertyType{},
		}
		table := &Stream{
			Name:                tableName,
			Schema:              schema,
			SupportedSyncModes:  []string{"full_refresh", "incremental"},
			Namespace:           keyspaceName,
			PrimaryKeys:         [][]string{},
			DefaultCursorFields: []string{},
			SourceDefinedCursor: true,
		}

		s.catalog.Streams = append(s.catalog.Streams, table)
		keyspace[tableName] = table
	}
}

func (s *AirbyteSchemaBuilder) OnColumns(keyspaceName, tableName string, columns []lib.MysqlColumn) {
	table, ok := s.tables[keyspaceName][tableName]
	if !ok {
		s.OnTable(keyspaceName, tableName)
		table = s.tables[keyspaceName][tableName]
	}

	for _, column := range columns {
		if column.IsPrimaryKey {
			table.PrimaryKeys = append(table.PrimaryKeys, []string{column.Name})
			table.DefaultCursorFields = []string{column.Name}
		}

		table.Schema.Properties[column.Name] = GetJsonSchemaType(column.Type, s.treatTinyIntAsBoolean)
	}
}

func (s *AirbyteSchemaBuilder) BuildCatalog() *Catalog {
	return s.catalog
}

// GetJsonSchemaType converts a mysqlType to an Airbyte type.
func GetJsonSchemaType(mysqlType string, treatTinyIntAsBoolean bool) PropertyType {
	// Support custom airbyte types documented here :
	// https://docs.airbyte.com/understanding-airbyte/supported-data-types/#the-types
	if strings.HasPrefix(mysqlType, "int") {
		return PropertyType{Type: "integer"}
	}

	if strings.HasPrefix(mysqlType, "decimal") || strings.HasPrefix(mysqlType, "double") {
		return PropertyType{Type: "number"}
	}

	if strings.HasPrefix(mysqlType, "bigint") {
		return PropertyType{Type: "string", AirbyteType: "big_integer"}
	}

	if strings.HasPrefix(mysqlType, "datetime") {
		return PropertyType{Type: "string", CustomFormat: "date-time", AirbyteType: "timestamp_without_timezone"}
	}

	if mysqlType == "tinyint(1)" {
		if treatTinyIntAsBoolean {
			return PropertyType{Type: "boolean"}
		}

		return PropertyType{Type: "integer"}
	}

	switch mysqlType {
	case "date":
		return PropertyType{Type: "string", AirbyteType: "date"}
	case "datetime":
		return PropertyType{Type: "string", AirbyteType: "timestamp_without_timezone"}
	default:
		return PropertyType{Type: "string"}
	}
}
