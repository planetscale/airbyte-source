package internal

import (
	"testing"
	
	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

func TestUserDefinedCursorIntegration(t *testing.T) {
	ped := PlanetScaleEdgeDatabase{}
	
	// Simulate table schema with id, name, and updated_at columns
	fields := []*query.Field{
		{Name: "id", Type: query.Type_INT32},
		{Name: "name", Type: query.Type_VARCHAR},
		{Name: "updated_at", Type: query.Type_DATETIME},
	}
	
	// Test data: rows with different updated_at values
	testCases := []struct {
		name           string
		row            []sqltypes.Value
		lastCursorValue interface{}
		expectInclude  bool
		description    string
	}{
		{
			name: "Include: Row with newer timestamp",
			row: []sqltypes.Value{
				sqltypes.NewInt32(1),
				sqltypes.NewVarChar("John"),
				sqltypes.NewVarChar("2024-01-15 12:00:00"),
			},
			lastCursorValue: "2024-01-15 10:00:00",
			expectInclude:   true,
			description:     "Row should be included because updated_at > last cursor",
		},
		{
			name: "Exclude: Row with older timestamp",
			row: []sqltypes.Value{
				sqltypes.NewInt32(2),
				sqltypes.NewVarChar("Jane"),
				sqltypes.NewVarChar("2024-01-15 08:00:00"),
			},
			lastCursorValue: "2024-01-15 10:00:00",
			expectInclude:   false,
			description:     "Row should be excluded because updated_at < last cursor",
		},
		{
			name: "Exclude: Row with equal timestamp",
			row: []sqltypes.Value{
				sqltypes.NewInt32(3),
				sqltypes.NewVarChar("Bob"),
				sqltypes.NewVarChar("2024-01-15 10:00:00"),
			},
			lastCursorValue: "2024-01-15 10:00:00",
			expectInclude:   false,
			description:     "Row should be excluded because updated_at = last cursor",
		},
		{
			name: "Include: First sync (no previous cursor)",
			row: []sqltypes.Value{
				sqltypes.NewInt32(4),
				sqltypes.NewVarChar("Alice"),
				sqltypes.NewVarChar("2024-01-01 00:00:00"),
			},
			lastCursorValue: nil,
			expectInclude:   true,
			description:     "Row should be included in first sync",
		},
		{
			name: "Include: Row with null cursor value",
			row: []sqltypes.Value{
				sqltypes.NewInt32(5),
				sqltypes.NewVarChar("Charlie"),
				sqltypes.NULL,
			},
			lastCursorValue: "2024-01-15 10:00:00",
			expectInclude:   true,
			description:     "Row with NULL updated_at should be included",
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var lastCursor *SerializedCursor
			if tc.lastCursorValue != nil {
				lastCursor = &SerializedCursor{
					UserDefinedCursorValue: tc.lastCursorValue,
				}
			}
			
			_, shouldInclude := ped.shouldIncludeRowBasedOnCursor(
				fields,
				tc.row,
				"updated_at",
				lastCursor,
			)
			
			assert.Equal(t, tc.expectInclude, shouldInclude, tc.description)
		})
	}
}

func TestUpdateMaxCursorValueIntegration(t *testing.T) {
	ped := PlanetScaleEdgeDatabase{}
	
	// Simulate processing multiple rows and tracking max cursor
	var maxCursor interface{}
	
	// Process rows in order
	rows := []string{
		"2024-01-15 09:00:00",
		"2024-01-15 12:00:00", // This should become max
		"2024-01-15 11:00:00",
		"2024-01-15 10:00:00",
	}
	
	for _, timestamp := range rows {
		maxCursor = ped.updateMaxCursorValue(maxCursor, timestamp)
	}
	
	// Should have tracked the maximum value
	assert.Equal(t, "2024-01-15 12:00:00", maxCursor)
}

func TestCursorWithDifferentDataTypes(t *testing.T) {
	ped := PlanetScaleEdgeDatabase{}
	
	t.Run("Integer cursor field", func(t *testing.T) {
		fields := []*query.Field{
			{Name: "id", Type: query.Type_INT32},
			{Name: "sequence_num", Type: query.Type_INT64},
		}
		
		row := []sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewInt64(1000),
		}
		
		lastCursor := &SerializedCursor{
			UserDefinedCursorValue: int64(999),
		}
		
		_, shouldInclude := ped.shouldIncludeRowBasedOnCursor(fields, row, "sequence_num", lastCursor)
		assert.True(t, shouldInclude, "Should include row with sequence_num 1000 > 999")
	})
	
	t.Run("Date cursor field", func(t *testing.T) {
		fields := []*query.Field{
			{Name: "id", Type: query.Type_INT32},
			{Name: "created_date", Type: query.Type_DATE},
		}
		
		row := []sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewVarChar("2024-01-16"),
		}
		
		lastCursor := &SerializedCursor{
			UserDefinedCursorValue: "2024-01-15",
		}
		
		_, shouldInclude := ped.shouldIncludeRowBasedOnCursor(fields, row, "created_date", lastCursor)
		assert.True(t, shouldInclude, "Should include row with date 2024-01-16 > 2024-01-15")
	})
}

func TestMissingCursorField(t *testing.T) {
	ped := PlanetScaleEdgeDatabase{}
	
	// Test behavior when cursor field doesn't exist in schema
	fields := []*query.Field{
		{Name: "id", Type: query.Type_INT32},
		{Name: "name", Type: query.Type_VARCHAR},
	}
	
	row := []sqltypes.Value{
		sqltypes.NewInt32(1),
		sqltypes.NewVarChar("Test"),
	}
	
	lastCursor := &SerializedCursor{
		UserDefinedCursorValue: "2024-01-15 10:00:00",
	}
	
	// Should include row when cursor field not found
	_, shouldInclude := ped.shouldIncludeRowBasedOnCursor(fields, row, "updated_at", lastCursor)
	assert.True(t, shouldInclude, "Should include row when cursor field not found in schema")
}