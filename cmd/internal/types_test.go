package internal

import (
	"encoding/base64"
	psdbconnect "github.com/planetscale/edge-gateway/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/psdb/core/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
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
