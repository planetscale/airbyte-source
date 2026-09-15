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
			name:      "filters_vt_hld_tables",
			tableName: "_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			filtered:  true,
		},
		{
			name:      "filters_vt_prg_tables",
			tableName: "_vt_prg_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			filtered:  true,
		},
		{
			name:      "filters_vt_evc_tables",
			tableName: "_vt_evc_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			filtered:  true,
		},
		{
			name:      "filters_vt_drp_tables",
			tableName: "_vt_drp_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			filtered:  true,
		},
		{
			name:      "filters_vt_vrp_tables",
			tableName: "_vt_vrp_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			filtered:  true,
		},
		{
			name:      "filters_vt_gho_tables",
			tableName: "_vt_gho_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			filtered:  true,
		},
		{
			name:      "filters_vt_ghc_tables",
			tableName: "_vt_ghc_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			filtered:  true,
		},
		{
			name:      "filters_vt_del_tables",
			tableName: "_vt_del_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			filtered:  true,
		},
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

// TestShardsForKeyspace_SiblingKeyspacePrefix covers the case where a database
// name ("trengo") shares a prefix with a sibling keyspace ("trengo_etl").
// "show vitess_shards like \"%trengo%\"" returned rows for both keyspaces, and
// TrimPrefix left "trengo_etl/-" in the shard list because that row does not
// start with "trengo/".
func TestShardsForKeyspace_SiblingKeyspacePrefix(t *testing.T) {
	rows := []string{
		"trengo/-",
		"trengo_etl/-",
	}

	t.Run("trengo returns only its own shard, not the sibling's", func(t *testing.T) {
		got := shardsForKeyspace("trengo", rows)
		assert.Equal(t, []string{"-"}, got)
		assert.NotContains(t, got, "trengo_etl/-", "the sibling keyspace's shard leaked in")
		for _, s := range got {
			assert.NotContains(t, s, "/", "shard name still has a keyspace prefix: %q", s)
		}
	})

	t.Run("trengo_etl returns only its own shard, prefix stripped", func(t *testing.T) {
		got := shardsForKeyspace("trengo_etl", rows)
		assert.Equal(t, []string{"-"}, got)
		assert.NotContains(t, got, "trengo/-", "the unsharded sibling keyspace's shard leaked in")
		for _, s := range got {
			assert.NotContains(t, s, "/", "shard name still has a keyspace prefix: %q", s)
		}
	})

	t.Run("unknown keyspace returns nothing", func(t *testing.T) {
		assert.Empty(t, shardsForKeyspace("other", rows))
	})

	t.Run("malformed rows without a separator are skipped", func(t *testing.T) {
		assert.Empty(t, shardsForKeyspace("trengo", []string{"trengo", ""}))
	})

	t.Run("sharded sibling is not mixed into an unsharded prefix keyspace", func(t *testing.T) {
		shardedRows := []string{
			"test/-",
			"test_sharded/-80",
			"test_sharded/80-",
		}
		assert.Equal(t, []string{"-"}, shardsForKeyspace("test", shardedRows))
		assert.Equal(t, []string{"-80", "80-"}, shardsForKeyspace("test_sharded", shardedRows))
	})
}
