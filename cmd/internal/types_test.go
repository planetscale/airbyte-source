package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

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
