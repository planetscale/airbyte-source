package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterNames_CanFilterInternalVitessTables(t *testing.T) {
	var tests = []struct {
		name      string
		tableName string
		filtered  bool
	}{
		{
			name:      "filters_vrepl_tables",
			tableName: "_750a3e1f_e6f3_5249_82af_82f5d325ecab_20240528153135_vrepl",
			filtered:  true,
		},
		{
			name:      "filters_vt_DROP_tables",
			tableName: "_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			filtered:  true,
		},
		{
			name:      "filters_vt_HOLD_tables",
			tableName: "_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			filtered:  true,
		},
		{
			name:      "filters_vt_EVAC_tables",
			tableName: "_vt_EVAC_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			filtered:  true,
		},
		{
			name:      "filters_vt_PURGE_tables",
			tableName: "_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			filtered:  true,
		},
		{
			name:      "does_not_filter_regular_table",
			tableName: "customers",
			filtered:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filteredResult := filterTable(tt.tableName)
			assert.Equal(t, tt.filtered, filteredResult)
		})
	}
}
