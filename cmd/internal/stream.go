package internal

import (
	"fmt"
	"strings"
)

type Stream struct {
	Name                string       `json:"name"`
	Schema              StreamSchema `json:"json_schema"`
	SupportedSyncModes  []string     `json:"supported_sync_modes"`
	Namespace           string       `json:"namespace"`
	PrimaryKeys         [][]string   `json:"source_defined_primary_key"`
	SourceDefinedCursor bool         `json:"source_defined_cursor"`
	DefaultCursorFields []string     `json:"default_cursor_field"`
}

func (s *Stream) GetSelectQuery() string {
	var columns []string
	for name := range s.Schema.Properties {
		columns = append(columns, name)
	}
	return fmt.Sprintf("Select %v from %v", strings.Join(columns, ","), s.Name)
}
