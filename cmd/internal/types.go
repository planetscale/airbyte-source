package internal

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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
	TRACE             = "TRACE"
)

const (
	TRACE_TYPE_STREAM_STATUS = "STREAM_STATUS"
)

const (
	STREAM_STATUS_STARTED    = "STARTED"
	STREAM_STATUS_COMPLETE   = "COMPLETE"
	STREAM_STATUS_INCOMPLETE = "INCOMPLETE"
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
				field := qr.Fields[idx]
				// Convert before parseValue so latin1 text is valid UTF-8
				// before JSON encoding, while enum/set index mapping still sees ASCII digits.
				val = convertTextToUTF8(val, field)
				parsedValue := parseValue(val, field.GetColumnType(), field.GetType(), ps)
				if parsedValue.isBool {
					record[columns[idx]] = parsedValue.boolValue
				} else if parsedValue.isNull {
					record[columns[idx]] = nil
				} else {
					record[columns[idx]] = parsedValue.sqlValue
				}
			}
		}
		data = append(data, record)
	}

	return data
}

// MySQL latin1 collation IDs. MySQL's latin1 is Windows-1252, not ISO-8859-1.
// VStream Field.Charset is the column collation ID (e.g. 8 = latin1_swedish_ci).
var latin1Collations = map[uint32]struct{}{
	5:  {}, // latin1_german1_ci
	8:  {}, // latin1_swedish_ci
	15: {}, // latin1_danish_ci
	31: {}, // latin1_german2_ci
	47: {}, // latin1_bin
	48: {}, // latin1_general_ci
	49: {}, // latin1_general_cs
	94: {}, // latin1_spanish_ci
}

// cp1252Map is the Unicode code point for each byte 0x80-0x9F in Windows-1252.
// All other latin1 bytes map directly to the same Unicode code point.
var cp1252Map = [32]rune{
	0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
	0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0x008D, 0x017D, 0x008F,
	0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
	0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178,
}

func isTextColumn(t query.Type) bool {
	switch t {
	case query.Type_CHAR, query.Type_VARCHAR, query.Type_TEXT, query.Type_ENUM, query.Type_SET:
		return true
	default:
		return false
	}
}

// convertTextToUTF8 rewrites latin1 text values to UTF-8 so JSON encoding
// does not replace non-ASCII bytes with U+FFFD or truncate the string.
// Binary columns (charset 63) and already-UTF-8 text are left unchanged.
func convertTextToUTF8(val sqltypes.Value, field *query.Field) sqltypes.Value {
	if val.IsNull() || field == nil || !isTextColumn(field.GetType()) {
		return val
	}
	if _, ok := latin1Collations[field.GetCharset()]; !ok {
		return val
	}

	converted := decodeWindows1252(val.Raw())
	newVal, err := sqltypes.NewValue(val.Type(), converted)
	if err != nil {
		return val
	}
	return newVal
}

func decodeWindows1252(src []byte) []byte {
	ascii := true
	for _, b := range src {
		if b >= 0x80 {
			ascii = false
			break
		}
	}
	if ascii {
		return src
	}

	dst := make([]byte, 0, len(src)+len(src)/2)
	for _, b := range src {
		var r rune
		switch {
		case b < 0x80, b >= 0xA0:
			r = rune(b)
		default:
			r = cp1252Map[b-0x80]
		}
		dst = utf8.AppendRune(dst, r)
	}
	return dst
}

type Value struct {
	sqlValue  sqltypes.Value
	boolValue bool
	isBool    bool
	isNull    bool
}

// After the initial COPY phase, enum and set values may appear as an index instead of a value.
// For example, a value might look like a "1" instead of "apple" in an enum('apple','banana','orange') column)
func parseValue(val sqltypes.Value, columnType string, queryColumnType query.Type, ps *PlanetScaleSource) Value {
	if val.IsNull() {
		return Value{
			isNull: true,
		}
	}

	switch queryColumnType {
	case query.Type_DATETIME, query.Type_DATE, query.Type_TIME:
		return formatISO8601(queryColumnType, val)
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
	case query.Type_DECIMAL:
		return Value{
			sqlValue: leadDecimalWithZero(val),
		}
	case query.Type_BINARY, query.Type_BIT, query.Type_BITNUM, query.Type_BLOB,
		query.Type_CHAR, query.Type_EXPRESSION,
		query.Type_FLOAT32, query.Type_FLOAT64, query.Type_GEOMETRY,
		query.Type_HEXNUM, query.Type_HEXVAL, query.Type_INT16, query.Type_INT24,
		query.Type_INT32, query.Type_INT64, query.Type_INT8, query.Type_JSON,
		query.Type_NULL_TYPE, query.Type_TEXT, query.Type_TIMESTAMP,
		query.Type_TUPLE, query.Type_UINT16, query.Type_UINT24, query.Type_UINT32,
		query.Type_UINT64, query.Type_UINT8, query.Type_VARBINARY,
		query.Type_VARCHAR, query.Type_YEAR:
		// No special handling.
	default:
		panic(fmt.Sprintf("unexpected query.Type: %#v", queryColumnType))
	}

	if strings.ToLower(columnType) == "tinyint(1)" && !ps.Options.DoNotTreatTinyIntAsBoolean {
		return mapTinyIntToBool(val)
	}

	return Value{
		sqlValue: val,
	}
}

func leadDecimalWithZero(val sqltypes.Value) sqltypes.Value {
	if !val.IsDecimal() {
		panic("non-decimal value")
	}
	valS := val.ToString()
	if strings.HasPrefix(valS, ".") || strings.HasPrefix(valS, "-.") {
		var newVal sqltypes.Value
		var err error
		if strings.HasPrefix(valS, ".") {
			newVal, err = sqltypes.NewValue(val.Type(), fmt.Appendf(nil, "0%s", valS))
		} else {
			newVal, err = sqltypes.NewValue(val.Type(), fmt.Appendf(nil, "-0%s", valS[1:]))
		}
		if err != nil {
			panic(fmt.Sprintf("failed to reconstruct decimal with leading zero: %v", err))
		}
		return newVal
	}
	return val
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

func formatISO8601(mysqlType query.Type, value sqltypes.Value) Value {
	var formatString string
	var layout string

	switch mysqlType {
	case query.Type_DATE:
		formatString = "2006-01-02"
		layout = time.DateOnly

	case query.Type_DATETIME:
		formatString = "2006-01-02 15:04:05"
		layout = "2006-01-02T15:04:05.000000" // No timezone offset
	case query.Type_NULL_TYPE, query.Type_INT8, query.Type_UINT8,
		query.Type_INT16, query.Type_UINT16, query.Type_INT24,
		query.Type_UINT24, query.Type_INT32, query.Type_UINT32,
		query.Type_INT64, query.Type_UINT64, query.Type_FLOAT32,
		query.Type_FLOAT64, query.Type_TIMESTAMP, query.Type_TIME,
		query.Type_YEAR, query.Type_DECIMAL, query.Type_TEXT, query.Type_BLOB,
		query.Type_VARCHAR, query.Type_VARBINARY, query.Type_CHAR,
		query.Type_BINARY, query.Type_BIT, query.Type_ENUM, query.Type_SET,
		query.Type_TUPLE, query.Type_GEOMETRY, query.Type_JSON,
		query.Type_EXPRESSION, query.Type_HEXNUM, query.Type_HEXVAL,
		query.Type_BITNUM:
		fallthrough
	default:
		formatString = "2006-01-02 15:04:05"
		layout = "2006-01-02T15:04:05.000000-07:00" // Timezone offset
	}

	var (
		mysqlTime time.Time
		err       error
	)

	if !value.IsNull() {
		parsedDatetime := value.ToString()
		// Check for zero date
		if parsedDatetime == "0000-00-00 00:00:00" || parsedDatetime == "0000-00-00" {
			// Use zero epoch time to represent non-null zero date
			mysqlTime = time.Unix(0, 0).UTC()
		} else {
			mysqlTime, err = time.Parse(formatString, parsedDatetime)
			if err != nil {
				// fallback to default value if datetime is not parseable
				return Value{
					sqlValue: value,
				}
			}
		}

	}

	iso8601Datetime := mysqlTime.Format(layout)
	formattedValue, _ := sqltypes.NewValue(mysqlType, []byte(iso8601Datetime))

	return Value{
		sqlValue: formattedValue,
	}
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

const (
	STATE_TYPE_STREAM = "STREAM"
)

type AirbyteStreamState struct {
	StreamDescriptor StreamDescriptor `json:"stream_descriptor"`
	StreamState      *ShardStates     `json:"stream_state"`
}

type AirbyteState struct {
	Type   string              `json:"type"`
	Stream *AirbyteStreamState `json:"stream,omitempty"`
}

type StreamDescriptor struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type AirbyteStreamStatus struct {
	StreamDescriptor StreamDescriptor `json:"stream_descriptor"`
	Status           string           `json:"status"`
}

type AirbyteTraceMessage struct {
	Type         string               `json:"type"`
	EmittedAt    float64              `json:"emitted_at"`
	StreamStatus *AirbyteStreamStatus `json:"stream_status,omitempty"`
}

type AirbyteMessage struct {
	Type             string               `json:"type"`
	Log              *AirbyteLogMessage   `json:"log,omitempty"`
	ConnectionStatus *ConnectionStatus    `json:"connectionStatus,omitempty"`
	Catalog          *Catalog             `json:"catalog,omitempty"`
	Record           *AirbyteRecord       `json:"record,omitempty"`
	State            *AirbyteState        `json:"state,omitempty"`
	Trace            *AirbyteTraceMessage `json:"trace,omitempty"`
}

// A map of starting GTIDs for every keyspace and shard
// i.e. { keyspace: { shard: gtid} }
type StartingGtids map[string]map[string]string
