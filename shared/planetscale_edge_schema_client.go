package shared

import (
	"context"
	"github.com/planetscale/airbyte-source/cmd/types"
	"strings"

	"github.com/pkg/errors"
)

type PlanetScaleDatabaseSchemaClient interface {
	DiscoverSchema(ctx context.Context, ps types.PlanetScaleSource) (types.Catalog, error)
}

type PlanetScaleEdgeDatabaseSchema struct {
	Logger types.AirbyteLogger
	Mysql  PlanetScaleEdgeMysqlAccess
}

func (p PlanetScaleEdgeDatabaseSchema) DiscoverSchema(ctx context.Context, psc types.PlanetScaleSource) (types.Catalog, error) {
	var c types.Catalog

	tables, err := p.Mysql.GetTableNames(ctx, psc)
	if err != nil {
		return c, errors.Wrap(err, "Unable to query database for schema")
	}

	for _, tableName := range tables {
		stream, err := p.getStreamForTable(ctx, psc, tableName)
		if err != nil {
			return c, errors.Wrapf(err, "unable to get stream for table %v", tableName)
		}
		c.Streams = append(c.Streams, stream)
	}
	return c, nil
}

func (p PlanetScaleEdgeDatabaseSchema) getStreamForTable(ctx context.Context, psc types.PlanetScaleSource, tableName string) (types.Stream, error) {
	schema := types.StreamSchema{
		Type:       "object",
		Properties: map[string]types.PropertyType{},
	}
	stream := types.Stream{
		Name:               tableName,
		Schema:             schema,
		SupportedSyncModes: []string{"full_refresh", "incremental"},
		Namespace:          psc.Database,
	}

	var err error
	stream.Schema.Properties, err = p.Mysql.GetTableSchema(ctx, psc, tableName)
	if err != nil {
		return stream, errors.Wrapf(err, "Unable to get column names & types for table %v", tableName)
	}

	// need this otherwise Airbyte will fail schema discovery for views
	// without primary keys.
	stream.PrimaryKeys = [][]string{}
	stream.DefaultCursorFields = []string{}

	primaryKeys, err := p.Mysql.GetTablePrimaryKeys(ctx, psc, tableName)
	if err != nil {
		return stream, errors.Wrapf(err, "unable to iterate primary keys for table %s", tableName)
	}
	for _, key := range primaryKeys {
		stream.PrimaryKeys = append(stream.PrimaryKeys, []string{key})
	}

	// pick the last key field as the default cursor field.
	if len(primaryKeys) > 0 {
		stream.DefaultCursorFields = append(stream.DefaultCursorFields, primaryKeys[len(primaryKeys)-1])
	}

	stream.SourceDefinedCursor = true
	return stream, nil
}

// Convert columnType to Airbyte type.
func getJsonSchemaType(mysqlType string, treatTinyIntAsBoolean bool) types.PropertyType {
	// Support custom airbyte types documented here :
	// https://docs.airbyte.com/understanding-airbyte/supported-data-types/#the-types
	if strings.HasPrefix(mysqlType, "int") {
		return types.PropertyType{Type: "integer"}
	}

	if strings.HasPrefix(mysqlType, "decimal") || strings.HasPrefix(mysqlType, "double") {
		return types.PropertyType{Type: "number"}
	}

	if strings.HasPrefix(mysqlType, "bigint") {
		return types.PropertyType{Type: "string", AirbyteType: "big_integer"}
	}

	if strings.HasPrefix(mysqlType, "datetime") {
		return types.PropertyType{Type: "string", CustomFormat: "date-time", AirbyteType: "timestamp_without_timezone"}
	}

	if mysqlType == "tinyint(1)" {
		if treatTinyIntAsBoolean {
			return types.PropertyType{Type: "boolean"}
		}

		return types.PropertyType{Type: "integer"}
	}

	switch mysqlType {
	case "date":
		return types.PropertyType{Type: "string", AirbyteType: "date"}
	case "datetime":
		return types.PropertyType{Type: "string", AirbyteType: "timestamp_without_timezone"}
	default:
		return types.PropertyType{Type: "string"}
	}
}
