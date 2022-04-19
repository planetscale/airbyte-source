package internal

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"io"
	"time"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"

	psdbdatav1 "github.com/planetscale/edge-gateway/proto/psdb/data_v1"
)

const (
	RECORD            = "RECORD"
	STATE             = "STATE"
	LOG               = "LOG"
	SPEC              = "SPEC"
	CONNECTION_STATUS = "CONNECTION_STATUS"
	CATALOG           = "CATALOG"
)

const (
	LOGLEVEL_FATAL    = "FATAL"
	LOGLEVEL_CRITICAL = "CRITICAL"
	LOGLEVEL_ERROR    = "ERROR"
	LOGLEVEL_WARN     = "WARN"
	LOGLEVEL_WARNING  = "WARNING"
	LOGLEVEL_INFO     = "INFO"
	LOGLEVEL_DEBUG    = "DEBUG"
	LOGLEVEL_TRACE    = "TRACE"
)

type PlanetScaleDatabase interface {
	CanConnect(ctx context.Context, ps PlanetScaleConnection) (bool, error)
	DiscoverSchema(ctx context.Context, ps PlanetScaleConnection) (Catalog, error)
	ListShards(ctx context.Context, ps PlanetScaleConnection) ([]string, error)
	Read(ctx context.Context, w io.Writer, ps PlanetScaleConnection, s ConfiguredStream, maxReadDuration time.Duration, tc *psdbdatav1.TableCursor) (*SerializedCursor, error)
}

type ConnectionStatus struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

type AirbyteLogMessage struct {
	Level   string `json:"level,omitempty"`
	Message string `json:"message,omitempty"`
}

const (
	SYNC_MODE_FULL_REFRESH = "full_refresh"
	SYNC_MODE_INCREMENTAL  = "incremental"
)

type PropertyType struct {
	Type string `json:"type"`
}

type StreamSchema struct {
	Type       string                  `json:"type"`
	Properties map[string]PropertyType `json:"properties"`
}

type Catalog struct {
	Streams []Stream `json:"streams"`
}

type ConfiguredStream struct {
	Stream   Stream `json:"stream"`
	SyncMode string `json:"sync_mode"`
}

func (cs ConfiguredStream) IncrementalSyncRequested() bool {
	return cs.SyncMode == "incremental"
}

func (cs ConfiguredStream) ResetRequested() bool {
	return cs.SyncMode == "append"
}

type ConfiguredCatalog struct {
	Streams []ConfiguredStream `json:"streams"`
}

type AirbyteRecord struct {
	Stream    string                 `json:"stream"`
	Namespace string                 `json:"namespace"`
	EmittedAt int64                  `json:"emitted_at"`
	Data      map[string]interface{} `json:"data"`
}

type SyncState struct {
	Streams map[string]ShardStates `json:"streams"`
}

type ShardStates struct {
	Shards map[string]*SerializedCursor `json:"shards"`
}

type SerializedCursor struct {
	Cursor      string `json:"cursor"`
	LastKnownPK string `json:"last_known_pk_state,omitempty"`
}

func (s SerializedCursor) ToTableCursor(table ConfiguredStream) (*psdbdatav1.TableCursor, error) {
	var (
		tc        psdbdatav1.TableCursor
		keyFields map[string]string
		lastPk    *query.QueryResult
	)

	err := json.Unmarshal([]byte(s.Cursor), &tc)
	if err != nil {
		return nil, errors.Wrap(err, "unable to deserialize table cursor")
	}

	if len(s.LastKnownPK) == 0 {
		return &tc, nil
	}

	err = json.Unmarshal([]byte(s.LastKnownPK), &keyFields)
	if err != nil {
		return nil, errors.Wrap(err, "unable to deserialize last known pk")
	}

	lastPk = &query.QueryResult{
		Fields: []*query.Field{},
		Rows:   []*query.Row{},
	}

	valueType := keyFields["type"]
	delete(keyFields, "type")
	queryType := query.Type(query.Type_value[valueType])
	for key, value := range keyFields {
		lastPk.Fields = append(lastPk.Fields, &query.Field{
			Name: key,
			Type: queryType,
		})

		lastPk.Rows = append(lastPk.Rows, &query.Row{
			Lengths: []int64{int64(len(value))},
			Values:  []byte(value),
		})
	}

	tc.LastKnownPk = lastPk

	return &tc, nil
}

func TableCursorToSerializedCursor(cursor *psdbdatav1.TableCursor) *SerializedCursor {
	var lastKnownPK []byte
	if cursor.LastKnownPk != nil {
		r := sqltypes.Proto3ToResult(cursor.LastKnownPk)
		data := QueryResultToRecords(r, true)
		if len(data) == 1 {
			lastKnownPK, _ = json.Marshal(data)
		}
	}

	b, _ := json.Marshal(psdbdatav1.TableCursor{
		Keyspace: cursor.Keyspace,
		Position: cursor.Position,
		Shard:    cursor.Shard,
	})

	sc := &SerializedCursor{
		Cursor:      string(b),
		LastKnownPK: string(lastKnownPK),
	}
	return sc
}

func QueryResultToRecords(qr *sqltypes.Result, includeTypes bool) []map[string]interface{} {
	data := make([]map[string]interface{}, 0, len(qr.Rows))

	columns := make([]string, 0, len(qr.Fields))
	for _, field := range qr.Fields {
		columns = append(columns, field.Name)
	}

	if includeTypes {
		columns = append(columns, "type")
	}

	for _, row := range qr.Rows {
		record := make(map[string]interface{})
		for idx, val := range row {
			if idx < len(columns) {
				record[columns[idx]] = val
			}
			if includeTypes {
				queryType := val.Type()
				record["type"] = query.Type_name[int32(queryType)]
			}
		}
		data = append(data, record)
	}

	return data
}

func SerializeCursor(cursor *psdbdatav1.TableCursor) *SerializedCursor {
	b, _ := json.Marshal(cursor)

	sc := &SerializedCursor{
		Cursor: string(b),
	}
	return sc
}

type AirbyteState struct {
	Data SyncState `json:"data"`
}

type AirbyteMessage struct {
	Type             string             `json:"type"`
	Log              *AirbyteLogMessage `json:"log,omitempty"`
	ConnectionStatus *ConnectionStatus  `json:"connectionStatus,omitempty"`
	Catalog          *Catalog           `json:"catalog,omitempty"`
	Record           *AirbyteRecord     `json:"record,omitempty"`
	State            *AirbyteState      `json:"state,omitempty"`
}

type SpecMessage struct {
	Type string `json:"type"`
	Spec Spec   `json:"spec"`
}

type ConnectionProperties struct {
	Host         ConnectionProperty `json:"host"`
	Shards       ConnectionProperty `json:"shards"`
	Database     ConnectionProperty `json:"database"`
	Username     ConnectionProperty `json:"username"`
	Password     ConnectionProperty `json:"password"`
	SyncDuration ConnectionProperty `json:"sync_duration"`
}

type ConnectionProperty struct {
	Description string      `json:"description"`
	Title       string      `json:"title"`
	Type        string      `json:"type"`
	Order       int         `json:"order"`
	IsSecret    bool        `json:"airbyte_secret"`
	Minimum     int         `json:"minimum"`
	Maximum     int         `json:"maximum"`
	Default     interface{} `json:"default"`
}

type ConnectionSpecification struct {
	Schema               string               `json:"$schema"`
	Title                string               `json:"title"`
	Type                 string               `json:"type"`
	Required             []string             `json:"required"`
	AdditionalProperties bool                 `json:"additionalProperties"`
	Properties           ConnectionProperties `json:"properties"`
}

type Spec struct {
	DocumentationURL              string                  `json:"documentationUrl"`
	ConnectionSpecification       ConnectionSpecification `json:"connectionSpecification"`
	SupportsIncremental           bool                    `json:"supportsIncremental"`
	SupportsNormalization         bool                    `json:"supportsNormalization"`
	SupportsDBT                   bool                    `json:"supportsDBT"`
	SupportedDestinationSyncModes []string                `json:"supported_destination_sync_modes"`
}
