package lib

import (
	"encoding/base64"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/psdb/core/codec"
)

type MysqlColumn struct {
	Name         string
	Type         string
	IsPrimaryKey bool
}

type SchemaBuilder interface {
	OnKeyspace(keyspaceName string)
	OnTable(keyspaceName, tableName string)
	OnColumns(keyspaceName, tableName string, columns []MysqlColumn)
}

func (s SerializedCursor) SerializedCursorToTableCursor() (*psdbconnect.TableCursor, error) {
	var tc psdbconnect.TableCursor
	decoded, err := base64.StdEncoding.DecodeString(s.Cursor)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decode table cursor")
	}

	err = codec.DefaultCodec.Unmarshal(decoded, &tc)
	if err != nil {
		return nil, errors.Wrap(err, "unable to deserialize table cursor")
	}

	return &tc, nil
}

func TableCursorToSerializedCursor(cursor *psdbconnect.TableCursor) (*SerializedCursor, error) {
	d, err := codec.DefaultCodec.Marshal(cursor)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal table cursor to save staate.")
	}

	sc := &SerializedCursor{
		Cursor: base64.StdEncoding.EncodeToString(d),
	}
	return sc, nil
}

type SerializedCursor struct {
	Cursor string `json:"cursor"`
}

type ShardStates struct {
	Shards map[string]*SerializedCursor `json:"shards"`
}

type KeyspaceState struct {
	Streams map[string]ShardStates `json:"streams"`
}

type SyncState struct {
	Keyspaces map[string]KeyspaceState `json:"keyspaces"`
}
