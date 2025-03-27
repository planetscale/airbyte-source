package internal

import (
	"encoding/base64"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/psdb/core/codec"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
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
	Type         []string `json:"type,omitempty"`
	CustomFormat string   `json:"format,omitempty"`
	AirbyteType  string   `json:"airbyte_type,omitempty"`
}

type OneOfType struct {
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

func QueryResultToRecords(qr *sqltypes.Result, ps *PlanetScaleSource) []map[string]interface{} {
	data := make([]map[string]interface{}, 0, len(qr.Rows))
	columns := make([]string, 0, len(qr.Fields))
	for _, field := range qr.Fields {
		columns = append(columns, field.Name)
	}

	for _, row := range qr.Rows {
		record := make(map[string]interface{})
		for idx, val := range row {
			if idx < len(columns) {
				parsedValue := parseValue(val, qr.Fields[idx].GetColumnType(), qr.Fields[idx].GetType(), ps)
				if parsedValue.isBool {
					record[columns[idx]] = parsedValue.boolValue
				} else {
					record[columns[idx]] = parsedValue.sqlValue
				}
			}
		}
		data = append(data, record)
	}

	return data
}

type Value struct {
	sqlValue  sqltypes.Value
	boolValue bool
	isBool    bool
}

// After the initial COPY phase, enum and set values may appear as an index instead of a value.
// For example, a value might look like a "1" instead of "apple" in an enum('apple','banana','orange') column)
func parseValue(val sqltypes.Value, columnType string, queryColumnType query.Type, ps *PlanetScaleSource) Value {
	switch queryColumnType {
	case query.Type_DATETIME, query.Type_DATE, query.Type_TIME:
		return Value{
			sqlValue: formatISO8601(queryColumnType, val),
		}
	case query.Type_ENUM:
		values := parseEnumOrSetValues(columnType)
		return Value{
			sqlValue: mapEnumValue(val, values),
		}
	case query.Type_SET:
		values := parseEnumOrSetValues(columnType)
		return Value{
			sqlValue: mapSetValue(val, values),
		}
	}

	if strings.ToLower(columnType) == "tinyint(1)" && !ps.Options.DoNotTreatTinyIntAsBoolean {
		return mapTinyIntToBool(val)
	}

	return Value{
		sqlValue: val,
	}
}

func mapTinyIntToBool(val sqltypes.Value) Value {
	sqlVal, err := val.ToBool()

	// Fallback to the original value if we can't convert to bool
	if err != nil {
		return Value{
			sqlValue: val,
		}
	}

	return Value{
		boolValue: sqlVal,
		isBool:    true,
	}
}

// Takes enum or set column type like ENUM('a','b','c') or SET('a','b','c')
// and returns a slice of values []string{'a', 'b', 'c'}
func parseEnumOrSetValues(columnType string) []string {
	values := []string{}

	re := regexp.MustCompile(`\((.+)\)`)
	res := re.FindString(columnType)
	res = strings.Trim(res, "(")
	res = strings.Trim(res, ")")
	for _, r := range strings.Split(res, ",") {
		values = append(values, strings.Trim(r, "'"))
	}

	return values
}

func formatISO8601(mysqlType query.Type, value sqltypes.Value) sqltypes.Value {
	parsedDatetime := value.ToString()

	var formatString string
	var layout string
	if mysqlType == query.Type_DATE {
		formatString = "2006-01-02"
		layout = time.DateOnly
	} else {
		formatString = "2006-01-02 15:04:05"
		layout = time.RFC3339
	}
	mysqlTime, err := time.Parse(formatString, parsedDatetime)
	if err != nil {
		// fallback to default value if datetime is not parseable
		return value
	}
	iso8601Datetime := mysqlTime.Format(layout)
	formattedValue, _ := sqltypes.NewValue(value.Type(), []byte(iso8601Datetime))
	return formattedValue
}

func mapSetValue(value sqltypes.Value, values []string) sqltypes.Value {
	parsedValue := value.ToString()
	parsedInt, err := strconv.ParseInt(parsedValue, 10, 64)
	if err != nil {
		// if value is not an integer, we just return the original value
		return value
	}
	mappedValues := []string{}
	// SET mapping is stored as a binary value, i.e. 1001
	bytes := strconv.FormatInt(parsedInt, 2)
	numValues := len(bytes)
	// if the bit is ON, that means the value at that index is included in the SET
	for i, char := range bytes {
		if char == '1' {
			// bytes are in reverse order, the first bit represents the last value in the SET
			mappedValue := values[numValues-(i+1)]
			mappedValues = append([]string{mappedValue}, mappedValues...)
		}
	}

	// If we can't find the values, just return the original value
	if len(mappedValues) == 0 {
		return value
	}

	mappedValue, _ := sqltypes.NewValue(value.Type(), []byte(strings.Join(mappedValues, ",")))
	return mappedValue
}

func mapEnumValue(value sqltypes.Value, values []string) sqltypes.Value {
	parsedValue := value.ToString()
	index, err := strconv.ParseInt(parsedValue, 10, 64)
	if err != nil {
		// If value is not an integer (index), we just return the original value
		return value
	}

	// The index value of the empty string error value is 0
	if index == 0 {
		emptyValue, _ := sqltypes.NewValue(value.Type(), []byte(""))
		return emptyValue
	}

	for i, v := range values {
		if int(index-1) == i {
			mappedValue, _ := sqltypes.NewValue(value.Type(), []byte(v))
			return mappedValue
		}
	}

	// Just return the original value if we can't find the enum value
	return value
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

// A map of starting GTIDs for every keyspace and shard
// i.e. { keyspace: { shard: gtid} }
type StartingGtids map[string]map[string]string
