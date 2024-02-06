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

	output := QueryResultToRecords(&input)
	assert.Equal(t, 2, len(output))
	firstRow := output[0]
	assert.Equal(t, "active", firstRow["status"].(sqltypes.Value).ToString())
	assert.Equal(t, "San Jose,Oakland", firstRow["locations"].(sqltypes.Value).ToString())
	secondRow := output[1]
	assert.Equal(t, "active", secondRow["status"].(sqltypes.Value).ToString())
	assert.Equal(t, "San Francisco,Oakland", secondRow["locations"].(sqltypes.Value).ToString())
}
