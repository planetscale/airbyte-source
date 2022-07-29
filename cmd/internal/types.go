package internal

import (
	"encoding/base64"
	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/psdb/core/codec"
	"vitess.io/vitess/go/sqltypes"
)

const (
	RECORD            = "RECORD"
	STATE             = "STATE"
	LOG               = "LOG"
	CONNECTION_STATUS = "CONNECTION_STATUS"
	CATALOG           = "CATALOG"
)

const (
	LOGLEVEL_ERROR = "ERROR"
	LOGLEVEL_WARN  = "WARN"
	LOGLEVEL_INFO  = "INFO"
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
	Type        string `json:"type"`
	AirbyteType string `json:"airbyte_type,omitempty"`
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

func (s SerializedCursor) SerializedCursorToTableCursor(table ConfiguredStream) (*psdbconnect.TableCursor, error) {
	var (
		tc psdbconnect.TableCursor
	)
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

func QueryResultToRecords(qr *sqltypes.Result) []map[string]interface{} {
	data := make([]map[string]interface{}, 0, len(qr.Rows))

	columns := make([]string, 0, len(qr.Fields))
	for _, field := range qr.Fields {
		columns = append(columns, field.Name)
	}

	for _, row := range qr.Rows {
		record := make(map[string]interface{})
		for idx, val := range row {
			if idx < len(columns) {
				record[columns[idx]] = val
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

type ConnectionPropertyHash struct {
	Description string                             `json:"description"`
	Title       string                             `json:"title"`
	Type        string                             `json:"type"`
	Order       int                                `json:"order"`
	Options     []ConnectionPropertyHashProperties `json:"oneOf"`
}

type ConnectionPropertyHashProperties struct {
	DoNotTreatTinyIntAsBoolean ConnectionProperty `json:"do_not_treat_tiny_int_as_boolean"`
}

type CustomOptionsSpecification struct {
	Description string          `json:"description"`
	Title       string          `json:"title"`
	Type        string          `json:"type"`
	Order       int             `json:"order"`
	Options     []CustomOptions `json:"oneOf"`
}

type CustomOptions struct {
	Description string                  `json:"description"`
	Title       string                  `json:"title"`
	Type        string                  `json:"type"`
	Order       int                     `json:"order"`
	Properties  CustomOptionsProperties `json:"properties"`
}

type CustomOptionsProperties struct {
	DoNotTreatTinyIntAsBoolean ConnectionProperty `json:"do_not_treat_tiny_int_as_boolean"`
}

type ConnectionProperties struct {
	Host     ConnectionProperty         `json:"host"`
	Shards   ConnectionProperty         `json:"shards"`
	Database ConnectionProperty         `json:"database"`
	Username ConnectionProperty         `json:"username"`
	Password ConnectionProperty         `json:"password"`
	Options  CustomOptionsSpecification `json:"options"`
}

type ConnectionProperty struct {
	Description string      `json:"description"`
	Title       string      `json:"title"`
	Type        string      `json:"type"`
	Order       int         `json:"order"`
	IsSecret    bool        `json:"airbyte_secret,omitempty"`
	Minimum     int         `json:"minimum,omitempty"`
	Maximum     int         `json:"maximum,omitempty"`
	Default     interface{} `json:"default,omitempty"`
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
