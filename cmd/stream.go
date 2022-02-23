package cmd

import (
	"fmt"
	"strings"
)

type Stream struct {
	Name               string       `json:"name"`
	Schema             StreamSchema `json:"json_schema"`
	SupportedSyncModes []string     `json:"supported_sync_modes"`
	Namespace          string       `json:"namespace"`
}

func (s *Stream) GetSelectQuery() string {
	var columns []string
	for name := range s.Schema.Properties {
		columns = append(columns, name)
	}
	return fmt.Sprintf("Select %v from %v", strings.Join(columns, ","), s.Name)
}
