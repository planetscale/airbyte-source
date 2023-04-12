package airbyte_source

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/planetscale/airbyte-source/lib"

	"github.com/planetscale/airbyte-source/cmd/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscoverInvalidSource(t *testing.T) {
	tfr := testFileReader{
		content: []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"),
	}

	tcc := &lib.TestConnectClient{
		CanConnectFn: func(ctx context.Context, ps lib.PlanetScaleSource) error {
			return fmt.Errorf("[%v] is invalid", "username")
		},
	}

	b := bytes.NewBufferString("")
	discover := DiscoverCommand(&Helper{
		Connect:    tcc,
		FileReader: tfr,
		Logger:     internal.NewSerializer(b),
	})
	discover.SetArgs([]string{"config source.json"})

	discover.SetOut(b)
	discover.Flag("config").Value.Set("catalog.json")
	discover.Execute()

	var amsg internal.AirbyteMessage
	err := json.NewDecoder(b).Decode(&amsg)
	require.NoError(t, err)
	assert.Equal(t, internal.CONNECTION_STATUS, amsg.Type)
	assert.NotNil(t, amsg.ConnectionStatus)
	assert.Equal(t, "FAILED", amsg.ConnectionStatus.Status)
	assert.Contains(t, amsg.ConnectionStatus.Message, "[username] is invalid")
}

func TestDiscoverFailed(t *testing.T) {
	tfr := testFileReader{
		content: []byte("{\"host\": \"something.us-east-3.psdb.cloud\",\"database\":\"database\",\"username\":\"username\",\"password\":\"password\"}"),
	}
	tcc := &lib.TestConnectClient{
		CanConnectFn: func(ctx context.Context, ps lib.PlanetScaleSource) error {
			return nil
		},
	}
	tmysql := &lib.TestMysqlClient{
		BuildSchemaFn: func(ctx context.Context, psc lib.PlanetScaleSource, schemaBuilder lib.SchemaBuilder) error {
			return fmt.Errorf("unable to get catalog for %v", "keyspace")
		},
		CloseFn: func() error {
			return nil
		},
	}
	b := bytes.NewBufferString("")
	discover := DiscoverCommand(&Helper{
		Connect:    tcc,
		Mysql:      tmysql,
		FileReader: tfr,
		Logger:     internal.NewSerializer(b),
	})
	discover.SetArgs([]string{"config source.json"})

	discover.SetOut(b)
	discover.Flag("config").Value.Set("catalog.json")
	discover.Execute()
	var amsg internal.AirbyteMessage
	err := json.NewDecoder(b).Decode(&amsg)
	require.NoError(t, err)
	assert.Equal(t, internal.LOG, amsg.Type)
	require.NotNil(t, amsg.Log)
	assert.Equal(t, internal.LOGLEVEL_ERROR, amsg.Log.Level)
	assert.Equal(t, "PlanetScale Source :: Unable to discover database, failed with [unable to get catalog for keyspace]", amsg.Log.Message)
}

func TestDiscover_CanPickRightAirbyteType(t *testing.T) {
	tests := []struct {
		MysqlType             string
		JSONSchemaType        string
		AirbyteType           string
		TreatTinyIntAsBoolean bool
	}{
		{
			MysqlType:      "int(32)",
			JSONSchemaType: "integer",
			AirbyteType:    "",
		},
		{
			MysqlType:             "tinyint(1)",
			JSONSchemaType:        "boolean",
			AirbyteType:           "",
			TreatTinyIntAsBoolean: true,
		},
		{
			MysqlType:             "tinyint(1)",
			JSONSchemaType:        "integer",
			AirbyteType:           "",
			TreatTinyIntAsBoolean: false,
		},
		{
			MysqlType:      "bigint(16)",
			JSONSchemaType: "string",
			AirbyteType:    "big_integer",
		},
		{
			MysqlType:      "bigint unsigned",
			JSONSchemaType: "string",
			AirbyteType:    "big_integer",
		},
		{
			MysqlType:      "bigint zerofill",
			JSONSchemaType: "string",
			AirbyteType:    "big_integer",
		},
		{
			MysqlType:      "datetime",
			JSONSchemaType: "string",
			AirbyteType:    "timestamp_without_timezone",
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
	}

	for _, typeTest := range tests {
		t.Run(fmt.Sprintf("mysql_type_%v", typeTest.MysqlType), func(t *testing.T) {
			p := internal.GetJsonSchemaType(typeTest.MysqlType, typeTest.TreatTinyIntAsBoolean)
			assert.Equal(t, typeTest.AirbyteType, p.AirbyteType)
			assert.Equal(t, typeTest.JSONSchemaType, p.Type)
		})
	}
}
