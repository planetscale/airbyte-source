package internal

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSchemaBuilder_CanPickRightAirbyteType(t *testing.T) {
	var tests = []struct {
		MysqlType             string
		JSONSchemaType        string
		AirbyteType           string
		TreatTinyIntAsBoolean bool
	}{
		{
			MysqlType:      "int(11)",
			JSONSchemaType: "number",
			AirbyteType:    "integer",
		},
		{
			MysqlType:      "smallint(4)",
			JSONSchemaType: "number",
			AirbyteType:    "integer",
		},
		{
			MysqlType:      "mediumint(8)",
			JSONSchemaType: "number",
			AirbyteType:    "integer",
		},
		{
			MysqlType:             "tinyint",
			JSONSchemaType:        "number",
			AirbyteType:           "integer",
			TreatTinyIntAsBoolean: true,
		},
		{
			MysqlType:             "tinyint(1)",
			JSONSchemaType:        "boolean",
			AirbyteType:           "",
			TreatTinyIntAsBoolean: true,
		},
		{
			MysqlType:             "tinyint(1) unsigned",
			JSONSchemaType:        "boolean",
			AirbyteType:           "",
			TreatTinyIntAsBoolean: true,
		},
		{
			MysqlType:             "tinyint(1)",
			JSONSchemaType:        "number",
			AirbyteType:           "integer",
			TreatTinyIntAsBoolean: false,
		},
		{
			MysqlType:             "tinyint(1) unsigned",
			JSONSchemaType:        "number",
			AirbyteType:           "integer",
			TreatTinyIntAsBoolean: false,
		},
		{
			MysqlType:      "bigint(16)",
			JSONSchemaType: "number",
			AirbyteType:    "integer",
		},
		{
			MysqlType:      "bigint unsigned",
			JSONSchemaType: "number",
			AirbyteType:    "integer",
		},
		{
			MysqlType:      "bigint zerofill",
			JSONSchemaType: "number",
			AirbyteType:    "integer",
		},
		{
			MysqlType:      "datetime",
			JSONSchemaType: "string",
			AirbyteType:    "timestamp_without_timezone",
		},
		{
			MysqlType:      "datetime(6)",
			JSONSchemaType: "string",
			AirbyteType:    "timestamp_without_timezone",
		},
		{
			MysqlType:      "time",
			JSONSchemaType: "string",
			AirbyteType:    "time_without_timezone",
		},
		{
			MysqlType:      "time(6)",
			JSONSchemaType: "string",
			AirbyteType:    "time_without_timezone",
		},
		{
			MysqlType:      "date",
			JSONSchemaType: "string",
			AirbyteType:    "date",
		},
		{
			MysqlType:      "text",
			JSONSchemaType: "string",
			AirbyteType:    "",
		},
		{
			MysqlType:      "varchar(256)",
			JSONSchemaType: "string",
			AirbyteType:    "",
		},
		{
			MysqlType:      "decimal(12,5)",
			JSONSchemaType: "number",
			AirbyteType:    "",
		},
		{
			MysqlType:      "double",
			JSONSchemaType: "number",
			AirbyteType:    "",
		},
		{
			MysqlType:      "float(30)",
			JSONSchemaType: "number",
			AirbyteType:    "",
		},
	}

	for _, typeTest := range tests {

		t.Run(fmt.Sprintf("mysql_type_%v", typeTest.MysqlType), func(t *testing.T) {
			p := getAirbyteDataType(typeTest.MysqlType, typeTest.TreatTinyIntAsBoolean)
			assert.Equal(t, typeTest.AirbyteType, p.AirbyteType)
			assert.Equal(t, typeTest.JSONSchemaType, p.Type)
		})
	}
}
