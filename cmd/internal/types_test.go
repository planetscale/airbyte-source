package internal

import (
	"encoding/base64"
	"testing"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/psdb/core/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

func TestCanSerializeLastKnownState(t *testing.T) {
	emp_no := "49999"
	sc, err := TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:    "40-80",
		Keyspace: "connect",
		Position: "THIS_IS_A_GTID",
		LastKnownPk: &query.QueryResult{
			Fields: []*query.Field{
				{
					Type: sqltypes.Int64,
					Name: "emp_no",
				},
			},
			Rows: []*query.Row{
				{
					Lengths: []int64{int64(len(emp_no))},
					Values:  []byte(emp_no),
				},
			},
		},
	})
	require.NoError(t, err)

	decodedBytes, err := base64.StdEncoding.DecodeString(sc.Cursor)
	require.NoError(t, err)
	var tc psdbconnect.TableCursor
	err = codec.DefaultCodec.Unmarshal(decodedBytes, &tc)
	require.NoError(t, err)
	assert.NotNil(t, tc.LastKnownPk)
	assert.Equal(t, 1, len(tc.LastKnownPk.Fields))
	assert.Equal(t, 1, len(tc.LastKnownPk.Rows))
	assert.Equal(t, "emp_no", tc.LastKnownPk.Fields[0].Name)
	assert.Equal(t, sqltypes.Int64, tc.LastKnownPk.Fields[0].Type)
}

func TestCanUnmarshalLastKnownState(t *testing.T) {
	emp_no := "49999"
	lastKnownPK := &query.QueryResult{
		Fields: []*query.Field{
			{
				Type: sqltypes.Int64,
				Name: "emp_no",
			},
		},
		Rows: []*query.Row{
			{
				Lengths: []int64{int64(len(emp_no))},
				Values:  []byte(emp_no),
			},
		},
	}
	sc, err := TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:       "40-80",
		Keyspace:    "connect",
		Position:    "THIS_IS_A_GTID",
		LastKnownPk: lastKnownPK,
	})
	require.NoError(t, err)
	tc, err := sc.SerializedCursorToTableCursor(ConfiguredStream{})
	require.NoError(t, err)
	assert.Equal(t, "connect", tc.Keyspace)
	assert.Equal(t, "40-80", tc.Shard)
	assert.Equal(t, "THIS_IS_A_GTID", tc.Position)
	assert.Equal(t, lastKnownPK, tc.LastKnownPk)
}

func TestCanMapEnumAndSetValues(t *testing.T) {
	indexEnumValue, err := sqltypes.NewValue(query.Type_ENUM, []byte("1"))
	assert.NoError(t, err)
	indexSetValue, err := sqltypes.NewValue(query.Type_SET, []byte("24")) // 24 is decimal conversion of 11000 in binary
	assert.NoError(t, err)
	mappedEnumValue, err := sqltypes.NewValue(query.Type_ENUM, []byte("active"))
	assert.NoError(t, err)
	mappedSetValue, err := sqltypes.NewValue(query.Type_SET, []byte("San Francisco,Oakland"))
	assert.NoError(t, err)
	input := sqltypes.Result{
		Fields: []*query.Field{
			{Name: "customer_id", Type: sqltypes.Int64, ColumnType: "bigint"},
			{Name: "status", Type: sqltypes.Enum, ColumnType: "enum('active','inactive')"},
			{Name: "locations", Type: sqltypes.Set, ColumnType: "set('San Francisco','New York','London','San Jose','Oakland')"},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt64(1), indexEnumValue, indexSetValue},
			{sqltypes.NewInt64(2), mappedEnumValue, mappedSetValue},
		},
	}

	output := QueryResultToRecords(&input, &PlanetScaleSource{})
	assert.Equal(t, 2, len(output))
	firstRow := output[0]
	assert.Equal(t, "active", firstRow["status"].(sqltypes.Value).ToString())
	assert.Equal(t, "San Jose,Oakland", firstRow["locations"].(sqltypes.Value).ToString())
	secondRow := output[1]
	assert.Equal(t, "active", secondRow["status"].(sqltypes.Value).ToString())
	assert.Equal(t, "San Francisco,Oakland", secondRow["locations"].(sqltypes.Value).ToString())
}

func TestCanMapTinyIntValues(t *testing.T) {
	input := sqltypes.Result{
		Fields: []*query.Field{
			{Name: "verified", Type: query.Type_INT8, ColumnType: "tinyint(1)"},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt8(1)},
			{sqltypes.NewInt8(0)},
		},
	}

	output := QueryResultToRecords(&input, &PlanetScaleSource{
		Options: CustomSourceOptions{
			DoNotTreatTinyIntAsBoolean: false,
		},
	})

	assert.Equal(t, 2, len(output))
	firstRow := output[0]
	assert.Equal(t, true, firstRow["verified"].(bool))
	secondRow := output[1]
	assert.Equal(t, false, secondRow["verified"].(bool))

	input = sqltypes.Result{
		Fields: []*query.Field{
			{Name: "verified", Type: query.Type_INT8, ColumnType: "tinyint(1)"},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt8(1)},
			{sqltypes.NewInt8(0)},
		},
	}

	output = QueryResultToRecords(&input, &PlanetScaleSource{
		Options: CustomSourceOptions{
			DoNotTreatTinyIntAsBoolean: true,
		},
	})

	assert.Equal(t, 2, len(output))
	firstRow = output[0]
	assert.Equal(t, sqltypes.NewInt8(1), firstRow["verified"])
	secondRow = output[1]
	assert.Equal(t, sqltypes.NewInt8(0), secondRow["verified"])
}

func TestCanFormatISO8601Values(t *testing.T) {
	datetimeValue, err := sqltypes.NewValue(query.Type_DATETIME, []byte("2025-02-14 08:08:08"))
	assert.NoError(t, err)
	dateValue, err := sqltypes.NewValue(query.Type_DATE, []byte("2025-02-14"))
	assert.NoError(t, err)
	timestampValue, err := sqltypes.NewValue(query.Type_TIMESTAMP, []byte("2025-02-14 08:08:08"))
	assert.NoError(t, err)
	zeroDatetimeValue, err := sqltypes.NewValue(query.Type_DATETIME, []byte("0000-00-00 00:00:00"))
	assert.NoError(t, err)
	zeroDateValue, err := sqltypes.NewValue(query.Type_DATE, []byte("0000-00-00"))
	assert.NoError(t, err)
	zeroTimestampValue, err := sqltypes.NewValue(query.Type_TIMESTAMP, []byte("0000-00-00 00:00:00"))
	assert.NoError(t, err)
	input := sqltypes.Result{
		Fields: []*query.Field{
			{Name: "datetime_created_at", Type: sqltypes.Datetime, ColumnType: "datetime"},
			{Name: "date_created_at", Type: sqltypes.Date, ColumnType: "date"},
			{Name: "timestamp_created_at", Type: sqltypes.Time, ColumnType: "timestamp"},
		},
		Rows: [][]sqltypes.Value{
			{datetimeValue, dateValue, timestampValue},
			{sqltypes.NULL, sqltypes.NULL, sqltypes.NULL},
			{zeroDatetimeValue, zeroDateValue, zeroTimestampValue},
		},
	}

	output := QueryResultToRecords(&input, &PlanetScaleSource{})
	assert.Equal(t, 3, len(output))
	row := output[0]
	assert.Equal(t, "2025-02-14T08:08:08.000000+00:00", row["datetime_created_at"].(sqltypes.Value).ToString())
	assert.Equal(t, "2025-02-14", row["date_created_at"].(sqltypes.Value).ToString())
	assert.Equal(t, "2025-02-14T08:08:08.000000+00:00", row["timestamp_created_at"].(sqltypes.Value).ToString())
	nullRow := output[1]
	assert.Equal(t, nil, nullRow["datetime_created_at"])
	assert.Equal(t, nil, nullRow["date_created_at"])
	assert.Equal(t, nil, nullRow["timestamp_created_at"])
	zeroRow := output[2]
	assert.Equal(t, "1970-01-01T00:00:00.000000+00:00", zeroRow["datetime_created_at"].(sqltypes.Value).ToString())
	assert.Equal(t, "1970-01-01", zeroRow["date_created_at"].(sqltypes.Value).ToString())
	assert.Equal(t, "1970-01-01T00:00:00.000000+00:00", zeroRow["timestamp_created_at"].(sqltypes.Value).ToString())
}
