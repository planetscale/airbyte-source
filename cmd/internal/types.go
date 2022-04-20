package internal

import (
	"context"
	"github.com/pkg/errors"
	"github.com/planetscale/edge-gateway/common/grpccommon/codec"
	"io"
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
	Read(ctx context.Context, w io.Writer, ps PlanetScaleConnection, s ConfiguredStream, tc *psdbdatav1.TableCursor) (*SerializedCursor, error)
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
	Cursor string `json:"cursor"`
}

func (s SerializedCursor) SerializedCursorToTableCursor(table ConfiguredStream) (*psdbdatav1.TableCursor, error) {
	var (
		tc psdbdatav1.TableCursor
	)

	err := codec.DefaultCodec.Unmarshal([]byte(s.Cursor), &tc)
	if err != nil {
		return nil, errors.Wrap(err, "unable to deserialize table cursor")
	}

	return &tc, nil
}

func TableCursorToSerializedCursor(cursor *psdbdatav1.TableCursor) (*SerializedCursor, error) {
	d, err := codec.DefaultCodec.Marshal(cursor)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal table cursor to save staate.")
	}
	sc := &SerializedCursor{
		Cursor: string(d),
	}
	return sc, nil
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
	Host     ConnectionProperty `json:"host"`
	Shards   ConnectionProperty `json:"shards"`
	Database ConnectionProperty `json:"database"`
	Username ConnectionProperty `json:"username"`
	Password ConnectionProperty `json:"password"`
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
