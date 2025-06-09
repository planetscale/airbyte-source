package internal

import (
	"encoding/base64"
	"testing"
	
	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

func TestCompareCursorValues(t *testing.T) {
	ped := PlanetScaleEdgeDatabase{}
	
	tests := []struct {
		name     string
		newValue string
		lastValue interface{}
		expected int
	}{
		{
			name:     "new timestamp greater than last",
			newValue: "2024-01-02 00:00:00",
			lastValue: base64.StdEncoding.EncodeToString([]byte("2024-01-01 00:00:00")),
			expected: 1,
		},
		{
			name:     "new timestamp less than last",
			newValue: "2024-01-01 00:00:00",
			lastValue: base64.StdEncoding.EncodeToString([]byte("2024-01-02 00:00:00")),
			expected: -1,
		},
		{
			name:     "timestamps equal",
			newValue: "2024-01-01 00:00:00",
			lastValue: base64.StdEncoding.EncodeToString([]byte("2024-01-01 00:00:00")),
			expected: 0,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newVal := sqltypes.NewVarChar(tt.newValue)
			result := ped.compareCursorValues(newVal, tt.lastValue, query.Type_DATETIME)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShouldIncludeRowBasedOnCursor_Simple(t *testing.T) {
	ped := PlanetScaleEdgeDatabase{}
	
	fields := []*query.Field{
		{Name: "id", Type: query.Type_INT32},
		{Name: "updated_at", Type: query.Type_DATETIME},
	}
	
	// Test: Include row when cursor value is greater
	row := []sqltypes.Value{
		sqltypes.NewInt32(1),
		sqltypes.NewVarChar("2024-01-02 00:00:00"),
	}
	
	lastCursor := &SerializedCursor{
		UserDefinedCursorValue: base64.StdEncoding.EncodeToString([]byte("2024-01-01 00:00:00")),
	}
	
	value, include := ped.shouldIncludeRowBasedOnCursor(fields, row, "updated_at", lastCursor)
	assert.True(t, include)
	// Value should be base64-encoded
	decodedValue, _ := base64.StdEncoding.DecodeString(value.(string))
	assert.Equal(t, "2024-01-02 00:00:00", string(decodedValue))
	
	// Test: Exclude row when cursor value is less
	row2 := []sqltypes.Value{
		sqltypes.NewInt32(2),
		sqltypes.NewVarChar("2023-12-31 00:00:00"),
	}
	
	value2, include2 := ped.shouldIncludeRowBasedOnCursor(fields, row2, "updated_at", lastCursor)
	assert.False(t, include2)
	// Value should be base64-encoded
	decodedValue2, _ := base64.StdEncoding.DecodeString(value2.(string))
	assert.Equal(t, "2023-12-31 00:00:00", string(decodedValue2))
}