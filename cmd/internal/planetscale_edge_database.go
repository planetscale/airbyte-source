package internal

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/psdb/auth"
	grpcclient "github.com/planetscale/psdb/core/pool"
	clientoptions "github.com/planetscale/psdb/core/pool/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	vtmysql "vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

// PlanetScaleDatabase is a general purpose interface
// that defines all the data access methods needed for the PlanetScale Airbyte source to function.
type PlanetScaleDatabase interface {
	CanConnect(ctx context.Context, ps PlanetScaleSource) error
	DiscoverSchema(ctx context.Context, ps PlanetScaleSource) (Catalog, error)
	ListShards(ctx context.Context, ps PlanetScaleSource) ([]string, error)
	Read(ctx context.Context, w io.Writer, ps PlanetScaleSource, s ConfiguredStream, tc *psdbconnect.TableCursor, lastCursor *SerializedCursor) (*SerializedCursor, error)
	Close() error
}

// PlanetScaleEdgeDatabase is an implementation of the PlanetScaleDatabase interface defined above.
// It uses the mysql interface provided by PlanetScale for all schema/shard/tablet discovery and
// the grpc API for incrementally syncing rows from PlanetScale.
type PlanetScaleEdgeDatabase struct {
	Logger         AirbyteLogger
	Mysql          PlanetScaleEdgeMysqlAccess
	vtgateClientFn func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error)
}

func (p PlanetScaleEdgeDatabase) CanConnect(ctx context.Context, psc PlanetScaleSource) error {
	if err := p.checkEdgePassword(ctx, psc); err != nil {
		return errors.Wrap(err, "Unable to initialize Connect Session")
	}

	return p.Mysql.PingContext(ctx, psc)
}

func (p PlanetScaleEdgeDatabase) checkEdgePassword(ctx context.Context, psc PlanetScaleSource) error {
	if !strings.HasSuffix(psc.Host, ".connect.psdb.cloud") {
		return errors.New("This password is not connect-enabled, please ensure that your organization is enrolled in the Connect beta.")
	}
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, fmt.Sprintf("https://%v", psc.Host), nil)
	if err != nil {
		return err
	}

	_, err = http.DefaultClient.Do(req)
	if err != nil {
		return errors.New(fmt.Sprintf("The database %q, hosted at %q, is inaccessible from this process", psc.Database, psc.Host))
	}

	return nil
}

func (p PlanetScaleEdgeDatabase) DiscoverSchema(ctx context.Context, psc PlanetScaleSource) (Catalog, error) {
	var c Catalog

	tables, err := p.Mysql.GetTableNames(ctx, psc)
	if err != nil {
		return c, errors.Wrap(err, "Unable to query database for schema")
	}

	for _, tableName := range tables {
		stream, err := p.getStreamForTable(ctx, psc, tableName)
		if err != nil {
			return c, errors.Wrapf(err, "unable to get stream for table %v", tableName)
		}
		c.Streams = append(c.Streams, stream)
	}
	return c, nil
}

func (p PlanetScaleEdgeDatabase) getStreamForTable(ctx context.Context, psc PlanetScaleSource, tableName string) (Stream, error) {
	schema := StreamSchema{
		Type:       "object",
		Properties: map[string]PropertyType{},
	}
	stream := Stream{
		Name:               tableName,
		Schema:             schema,
		SupportedSyncModes: []string{"full_refresh", "incremental"},
		Namespace:          psc.Database,
	}

	var err error
	stream.Schema.Properties, err = p.Mysql.GetTableSchema(ctx, psc, tableName)
	if err != nil {
		return stream, errors.Wrapf(err, "Unable to get column names & types for table %v", tableName)
	}

	// need this otherwise Airbyte will fail schema discovery for views
	// without primary keys.
	stream.PrimaryKeys = [][]string{}
	stream.DefaultCursorFields = []string{}

	primaryKeys, err := p.Mysql.GetTablePrimaryKeys(ctx, psc, tableName)
	if err != nil {
		return stream, errors.Wrapf(err, "unable to iterate primary keys for table %s", tableName)
	}
	for _, key := range primaryKeys {
		stream.PrimaryKeys = append(stream.PrimaryKeys, []string{key})
	}

	// pick the last key field as the default cursor field.
	if len(primaryKeys) > 0 {
		stream.DefaultCursorFields = append(stream.DefaultCursorFields, primaryKeys[len(primaryKeys)-1])
	}

	// Set SourceDefinedCursor to false to allow users to optionally define their own cursor
	// The connector will still support source-defined cursors when no cursor_field is provided
	stream.SourceDefinedCursor = false
	return stream, nil
}

// Convert columnType to Airbyte type.
func getJsonSchemaType(mysqlType string, treatTinyIntAsBoolean bool, nullable string) PropertyType {
	// Support custom airbyte types documented here :
	// https://docs.airbyte.com/understanding-airbyte/supported-data-types/#the-types
	var (
		jsonSchemaType string
		customFormat   string
		airbyteType    string
	)

	switch {
	case strings.HasPrefix(mysqlType, "tinyint(1)"):
		if treatTinyIntAsBoolean {
			jsonSchemaType = "boolean"
		} else {
			jsonSchemaType = "number"
			airbyteType = "integer"
		}
	case strings.HasPrefix(mysqlType, "int"), strings.HasPrefix(mysqlType, "smallint"), strings.HasPrefix(mysqlType, "mediumint"), strings.HasPrefix(mysqlType, "bigint"), strings.HasPrefix(mysqlType, "tinyint"):
		jsonSchemaType = "number"
		airbyteType = "integer"
	case strings.HasPrefix(mysqlType, "decimal"), strings.HasPrefix(mysqlType, "double"), strings.HasPrefix(mysqlType, "float"):
		jsonSchemaType = "number"
	case strings.HasPrefix(mysqlType, "datetime"):
		jsonSchemaType = "string"
		customFormat = "date-time"
		airbyteType = "timestamp_without_timezone"
	case strings.HasPrefix(mysqlType, "timestamp"):
		jsonSchemaType = "string"
		customFormat = "date-time"
		airbyteType = "timestamp_with_timezone"
	case strings.HasPrefix(mysqlType, "date"):
		jsonSchemaType = "string"
		customFormat = "date"
		airbyteType = "date"
	case strings.HasPrefix(mysqlType, "time"):
		jsonSchemaType = "string"
		customFormat = "time"
		airbyteType = "time_with_timezone"
	default:
		jsonSchemaType = "string"
	}

	propertyType := PropertyType{
		Type:         []string{jsonSchemaType},
		CustomFormat: customFormat,
		AirbyteType:  airbyteType,
	}

	if strings.ToLower(nullable) == "yes" {
		propertyType.Type = []string{"null", propertyType.Type[0]}
	}

	return propertyType
}

func (p PlanetScaleEdgeDatabase) Close() error {
	return p.Mysql.Close()
}

// shouldIncludeRowBasedOnCursor checks if a row should be included based on user-defined cursor
func (p PlanetScaleEdgeDatabase) shouldIncludeRowBasedOnCursor(fields []*query.Field, row []sqltypes.Value, cursorFieldName string, lastCursor *SerializedCursor) (interface{}, bool) {
	// Find the cursor field index
	cursorFieldIndex := -1
	for i, field := range fields {
		if field.Name == cursorFieldName {
			cursorFieldIndex = i
			break
		}
	}
	
	if cursorFieldIndex == -1 || cursorFieldIndex >= len(row) {
		// Cursor field not found, include the row
		return nil, true
	}
	
	cursorValue := row[cursorFieldIndex]
	if cursorValue.IsNull() {
		// Null cursor values are included
		return nil, true
	}
	
	// If this is the first sync or no previous cursor value, include all rows
	if lastCursor == nil || lastCursor.UserDefinedCursorValue == nil {
		// Return base64-encoded value for consistency
		return base64.StdEncoding.EncodeToString([]byte(cursorValue.ToString())), true
	}
	
	// Compare cursor values
	comparison := p.compareCursorValues(cursorValue, lastCursor.UserDefinedCursorValue, fields[cursorFieldIndex].Type)
	
	// Include rows where cursor value is greater than last seen value
	// Return base64-encoded value for consistency
	return base64.StdEncoding.EncodeToString([]byte(cursorValue.ToString())), comparison > 0
}

// compareCursorValues compares two cursor values based on their type
func (p PlanetScaleEdgeDatabase) compareCursorValues(newValue sqltypes.Value, lastValue interface{}, fieldType query.Type) int {
	var lastValueStr string
	switch v := lastValue.(type) {
	case string:
		// Always expect base64-encoded values for user-defined cursors
		if decoded, err := base64.StdEncoding.DecodeString(v); err == nil {
			lastValueStr = string(decoded)
		} else {
			// If decode fails, use the string as-is (shouldn't happen with our encoding)
			lastValueStr = v
		}
	case []byte:
		lastValueStr = string(v)
	default:
		lastValueStr = fmt.Sprintf("%v", lastValue)
	}
	
	// Convert to sqltypes.Value for comparison
	lastSQLValue, err := sqltypes.NewValue(newValue.Type(), []byte(lastValueStr))
	if err != nil {
		// If we can't convert, include the row
		return 1
	}
	
	// Handle numeric types specially
	switch fieldType {
	case query.Type_INT8, query.Type_INT16, query.Type_INT24, query.Type_INT32, query.Type_INT64,
		query.Type_UINT8, query.Type_UINT16, query.Type_UINT24, query.Type_UINT32, query.Type_UINT64:
		// Compare as integers
		newInt, err1 := newValue.ToInt64()
		lastInt, err2 := lastSQLValue.ToInt64()
		if err1 == nil && err2 == nil {
			if newInt > lastInt {
				return 1
			} else if newInt < lastInt {
				return -1
			}
			return 0
		}
	case query.Type_NULL_TYPE, query.Type_FLOAT32, query.Type_FLOAT64, query.Type_TIMESTAMP,
		query.Type_DATE, query.Type_TIME, query.Type_DATETIME, query.Type_YEAR, query.Type_DECIMAL,
		query.Type_TEXT, query.Type_BLOB, query.Type_VARCHAR, query.Type_VARBINARY, query.Type_CHAR,
		query.Type_BINARY, query.Type_BIT, query.Type_ENUM, query.Type_SET, query.Type_TUPLE,
		query.Type_GEOMETRY, query.Type_JSON, query.Type_EXPRESSION, query.Type_HEXNUM,
		query.Type_HEXVAL, query.Type_BITNUM:
		// For all other types, fall through to string comparison
	}
	
	// For all other types (dates, timestamps, strings), compare as strings
	newStr := newValue.ToString()
	lastStr := lastSQLValue.ToString()
	
	if newStr > lastStr {
		return 1
	} else if newStr < lastStr {
		return -1
	}
	return 0
}

// updateMaxCursorValue updates the maximum cursor value seen
func (p PlanetScaleEdgeDatabase) updateMaxCursorValue(currentMax interface{}, newValue interface{}) interface{} {
	if currentMax == nil {
		return newValue
	}
	
	currentSQLValue, err1 := sqltypes.InterfaceToValue(currentMax)
	newSQLValue, err2 := sqltypes.InterfaceToValue(newValue)
	
	if err1 != nil || err2 != nil {
		// If conversion fails, keep current max
		return currentMax
	}
	
	// Compare by converting to string
	if newSQLValue.ToString() > currentSQLValue.ToString() {
		return newValue
	}
	
	return currentMax
}

func (p PlanetScaleEdgeDatabase) ListShards(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	return p.Mysql.GetVitessShards(ctx, psc)
}

// Read streams rows from a table given a starting cursor.
// 1. We will get the latest vgtid for a given table in a shard when a sync session starts.
// 2. This latest vgtid is now the stopping point for this sync session.
// 3. Ask vstream to stream from the last known vgtid
// 4. When we reach the stopping point, read all rows available at this vgtid
// 5. End the stream when (a) a vgtid newer than latest vgtid is encountered or (b) the timeout kicks in.
func (p PlanetScaleEdgeDatabase) Read(ctx context.Context, w io.Writer, ps PlanetScaleSource, s ConfiguredStream, lastKnownPosition *psdbconnect.TableCursor, lastCursor *SerializedCursor) (*SerializedCursor, error) {
	var (
		err                     error
		sErr                    error
		currentSerializedCursor *SerializedCursor
		syncMode                string
		maxCursorValue          interface{}
	)
	
	// Initialize max cursor value from last sync if using user-defined cursor
	if len(s.CursorField) > 0 && lastCursor != nil && lastCursor.UserDefinedCursorValue != nil {
		maxCursorValue = lastCursor.UserDefinedCursorValue
		
		// Decode base64-encoded cursor value
		if strVal, ok := maxCursorValue.(string); ok {
			if decoded, err := base64.StdEncoding.DecodeString(strVal); err == nil {
				maxCursorValue = string(decoded)
			}
		}
		
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("Using user-defined cursor field %v with last value: %v", s.CursorField[0], maxCursorValue))
	}

	tabletType := psdbconnect.TabletType_primary
	if ps.UseRdonly {
		tabletType = psdbconnect.TabletType_batch
	} else if ps.UseReplica {
		tabletType = psdbconnect.TabletType_replica
	}

	currentPosition := lastKnownPosition
	if currentPosition.Position == "" || currentPosition.LastKnownPk != nil {
		syncMode = "full"
	} else {
		syncMode = "incremental"
	}

	table := s.Stream
	timeout := 5 * time.Minute
	if timeoutSeconds := ps.TimeoutSeconds; timeoutSeconds != nil {
		timeout = time.Duration(*timeoutSeconds) * time.Second
	}
	maxRetries := ps.MaxRetries

	preamble := fmt.Sprintf("[%v:%v:%v shard : %v] ", table.Namespace, TabletTypeToString(tabletType), table.Name, currentPosition.Shard)

	p.Logger.Log(LOGLEVEL_INFO, preamble+"Peeking to see if there's any new GTIDs")
	stopPosition, lcErr := p.getStopCursorPosition(ctx, currentPosition.Shard, currentPosition.Keyspace, table, ps, tabletType)
	if lcErr != nil {
		p.Logger.Log(LOGLEVEL_ERROR, preamble+fmt.Sprintf("Error fetching latest cursor position: %+v", lcErr))
		return currentSerializedCursor, errors.Wrap(err, "Unable to get latest cursor position")
	}
	if stopPosition == "" {
		p.Logger.Log(LOGLEVEL_ERROR, preamble+fmt.Sprintf("Error fetching latest cursor position, was empty string: %+v", stopPosition))
		return currentSerializedCursor, errors.Wrap(err, "Unable to get latest cursor position")
	}

	// the last synced VGTID is not after the current VGTID
	if currentPosition.Position != "" && !positionAfter(stopPosition, currentPosition.Position) {
		p.Logger.Log(LOGLEVEL_INFO, preamble+"No new GTIDs found, exiting")
		return TableCursorToSerializedCursor(currentPosition)
	}
	p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf(preamble+"New GTIDs found, syncing for %v", timeout))

	var syncCount uint = 0
	totalRecordCount := 0

	for {
		syncCount += 1
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sStarting sync #%v", preamble, syncCount))
		newPosition, recordCount, newMaxCursorValue, err := p.sync(ctx, syncMode, currentPosition, stopPosition, table, ps, tabletType, timeout, s, lastCursor, maxCursorValue)
		totalRecordCount += recordCount
		
		// Update max cursor value if we found a larger one
		if newMaxCursorValue != nil {
			maxCursorValue = newMaxCursorValue
		}
		
		currentSerializedCursor, sErr = TableCursorToSerializedCursor(currentPosition)
		if sErr != nil {
			// if we failed to serialize here, we should bail.
			return currentSerializedCursor, errors.Wrap(sErr, "unable to serialize current position")
		}
		
		// Add user-defined cursor value if applicable
		if len(s.CursorField) > 0 && maxCursorValue != nil {
			currentSerializedCursor.UserDefinedCursorValue = maxCursorValue
		}
		if err != nil {
			if s, ok := status.FromError(err); ok {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%v%v records synced after %v syncs. Got error [%v], returning with cursor [%v] after gRPC error", preamble, totalRecordCount, syncCount, s.Code(), currentPosition))
				if syncCount >= maxRetries {
					return currentSerializedCursor, nil
				}
			} else if errors.Is(err, io.EOF) {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%vFinished reading %v records after %v syncs for table [%v]", preamble, totalRecordCount, syncCount, table.Name))
				return currentSerializedCursor, nil
			} else {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%v%v records synced after %v syncs. Got error [%v], returning with cursor [%v] after server timeout", preamble, totalRecordCount, syncCount, err, currentPosition))
				return currentSerializedCursor, err
			}
		}
		currentPosition = newPosition
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%vContinuing to next sync #%v. Set next sync start position to [%+v].", preamble, syncCount+1, currentPosition))
	}
}

func (p PlanetScaleEdgeDatabase) sync(ctx context.Context, syncMode string, tc *psdbconnect.TableCursor, stopPosition string, s Stream, ps PlanetScaleSource, tabletType psdbconnect.TabletType, timeout time.Duration, configuredStream ConfiguredStream, lastCursor *SerializedCursor, maxCursorValue interface{}) (*psdbconnect.TableCursor, int, interface{}, error) {
	preamble := fmt.Sprintf("[%v:%v:%v shard : %v] ", s.Namespace, TabletTypeToString(tabletType), s.Name, tc.Shard)

	defer p.Logger.Flush()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var (
		err          error
		vtgateClient vtgateservice.VitessClient
		fields       []*query.Field
	)

	vtgateClient, conn, err := p.initializeVTGateClient(ctx, ps)
	if err != nil {
		return tc, 0, maxCursorValue, err
	}
	if conn != nil {
		defer conn.Close()
	}

	if tc.LastKnownPk != nil && !ps.UseGTIDWithTablePKs {
		tc.Position = ""
	}

	isFullSync := syncMode == "full"
	vtgateReq := buildVStreamRequest(tabletType, s.Name, tc.Shard, tc.Keyspace, tc.Position, tc.LastKnownPk)
	p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sRequesting VStream with %+v", preamble, vtgateReq))

	if isFullSync {
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sWill stop once COPY COMPLETED event is seen.", preamble))
	} else {
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sWill stop once stop position [%+v] is found.", preamble, stopPosition))
	}

	c, err := vtgateClient.VStream(ctx, vtgateReq)
	if err != nil {
		p.Logger.Log(LOGLEVEL_ERROR, fmt.Sprintf("%sExiting sync due to client sync error: %+v", preamble, err))
		return tc, 0, maxCursorValue, err
	}

	keyspaceOrDatabase := s.Namespace
	if keyspaceOrDatabase == "" {
		keyspaceOrDatabase = ps.Database
	}

	copyCompletedSeen := false
	// Can finish sync once we've synced to the stop position, or finished the VStream COPY phase
	canFinishSync := false
	resultCount := 0

	for {
		res, err := c.Recv()
		if err != nil {
			if s, ok := status.FromError(err); ok && s.Code() == codes.DeadlineExceeded {
				// No next VGTID found
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sExiting sync and flushing records because no new VGTID found after last position or deadline exceeded %+v", preamble, tc))
				return tc, resultCount, maxCursorValue, err
			} else if errors.Is(err, io.EOF) {
				// EOF is an acceptable error indicating VStream is finished.
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sExiting sync and flushing records because EOF encountered at position %+v", preamble, tc))
				return tc, resultCount, maxCursorValue, io.EOF
			} else {
				p.Logger.Log(LOGLEVEL_ERROR, fmt.Sprintf("%sExiting sync and flushing records due to error: %+v", preamble, err))
				return tc, resultCount, maxCursorValue, err
			}
		}

		var rows []*query.QueryResult
		for _, event := range res.Events {
			switch event.Type {
			case binlogdata.VEventType_VGTID:
				vgtid := event.GetVgtid().ShardGtids[0]
				if vgtid != nil {
					tc.Position = vgtid.Gtid
					if vgtid.TablePKs != nil {
						tablePK := vgtid.TablePKs[0]
						if tablePK != nil {
							// Setting LastKnownPk allows a COPY phase to pick up where it left off
							lastPK := tablePK.Lastpk
							tc.LastKnownPk = lastPK
						} else {
							tc.LastKnownPk = nil
						}
					} else {
						tc.LastKnownPk = nil
					}
				}
			case binlogdata.VEventType_LASTPK:
				if event.LastPKEvent.TableLastPK != nil {
					// Only update last PK because we're in a COPY phase
					tc = &psdbconnect.TableCursor{
						Shard:       tc.Shard,
						Keyspace:    tc.Keyspace,
						LastKnownPk: event.LastPKEvent.TableLastPK.Lastpk,
						Position:    tc.Position,
					}
					p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sLASTPK event found, setting last PK to %+v", preamble, tc))
				}
			case binlogdata.VEventType_FIELD:
				// Save fields for processing
				fields = event.FieldEvent.Fields
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sFIELD event found, setting fields to %+v", preamble, fields))
			case binlogdata.VEventType_ROW:
				// Collect rows for processing
				for _, change := range event.RowEvent.RowChanges {
					if change.After != nil {
						rows = append(rows, &query.QueryResult{
							Fields: fields,
							Rows:   []*query.Row{change.After},
						})
					}
				}
			case binlogdata.VEventType_COPY_COMPLETED:
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sCOPY_COMPLETED event found, copy phase finished", preamble))
				copyCompletedSeen = true
			case binlogdata.VEventType_BEGIN, binlogdata.VEventType_COMMIT,
				binlogdata.VEventType_DDL, binlogdata.VEventType_DELETE,
				binlogdata.VEventType_GTID, binlogdata.VEventType_HEARTBEAT,
				binlogdata.VEventType_INSERT, binlogdata.VEventType_JOURNAL,
				binlogdata.VEventType_OTHER, binlogdata.VEventType_REPLACE,
				binlogdata.VEventType_ROLLBACK, binlogdata.VEventType_SAVEPOINT,
				binlogdata.VEventType_SET, binlogdata.VEventType_UNKNOWN,
				binlogdata.VEventType_UPDATE, binlogdata.VEventType_VERSION:
				// No special handling.
			default:
				panic(fmt.Sprintf("unexpected binlogdata.VEventType: %#v", event.Type))
			}
		}

		if isFullSync && copyCompletedSeen {
			p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sReady to finish sync and flush since copy phase completed or stop VGTID passed", preamble))
			canFinishSync = true
		}
		if !isFullSync && positionEqual(tc.Position, stopPosition) {
			p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sReady to finish sync and flush since stop position [%+v] found", preamble, stopPosition))
			canFinishSync = true
		}

		if len(rows) > 0 {
			for _, result := range rows {
				qr := sqltypes.Proto3ToResult(result)
				for _, row := range qr.Rows {
					// Handle user-defined cursor filtering
					if len(configuredStream.CursorField) > 0 {
						cursorFieldName := configuredStream.CursorField[0]
						cursorValue, shouldInclude := p.shouldIncludeRowBasedOnCursor(fields, row, cursorFieldName, lastCursor)
						
						if !shouldInclude {
							var lastCursorVal interface{}
							if lastCursor != nil {
								lastCursorVal = lastCursor.UserDefinedCursorValue
							}
							p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("Filtering out row with %s=%v (last cursor=%v)", 
								cursorFieldName, cursorValue, lastCursorVal))
							continue // Skip this row
						}
						
						// Update max cursor value if this row has a larger value
						if cursorValue != nil {
							oldMax := maxCursorValue
							maxCursorValue = p.updateMaxCursorValue(maxCursorValue, cursorValue)
							if oldMax != maxCursorValue {
								p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("Updated max cursor value from %v to %v", oldMax, maxCursorValue))
							}
						}
					}
					
					resultCount += 1
					sqlResult := &sqltypes.Result{
						Fields: fields,
					}
					sqlResult.Rows = append(sqlResult.Rows, row)
					// Results queued to Airbyte here, and flushed at the end of sync()
					p.printQueryResult(sqlResult, keyspaceOrDatabase, s.Name, &ps)
				}
			}
		}

		// Exit sync and flush records once the VGTID position is at or past the desired stop position, and we're no longer waiting for COPY phase to complete
		if canFinishSync {
			if isFullSync {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sExiting full sync and flushing records because COPY_COMPLETED event was seen, current position is %+v, stop position is %+v", preamble, tc.Position, stopPosition))
			} else {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sExiting incremental sync and flushing records because current position %+v has reached or passed stop position %+v", preamble, tc.Position, stopPosition))
			}
			return tc, resultCount, maxCursorValue, io.EOF
		}
	}
}

func (p PlanetScaleEdgeDatabase) getStopCursorPosition(ctx context.Context, shard, keyspace string, s Stream, ps PlanetScaleSource, tabletType psdbconnect.TabletType) (string, error) {
	defer p.Logger.Flush()
	timeout := 45 * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var (
		err          error
		vtgateClient vtgateservice.VitessClient
	)

	vtgateClient, conn, err := p.initializeVTGateClient(ctx, ps)
	if err != nil {
		return "", err
	}
	if conn != nil {
		defer conn.Close()
	}

	vtgateReq := buildVStreamRequest(tabletType, s.Name, shard, keyspace, "current", nil)
	vtgateCursor, vtgateErr := vtgateClient.VStream(ctx, vtgateReq)

	if vtgateErr != nil {
		return "", nil
	}

	for {
		res, err := vtgateCursor.Recv()
		if err != nil {
			return "", err
		}

		if res.Events != nil {
			for _, event := range res.Events {
				if event.Type == binlogdata.VEventType_VGTID {
					gtid := event.Vgtid.ShardGtids[0].Gtid
					return gtid, nil
				}
			}
			return "", errors.New("unable to find VEvent of VGTID type to use as stop cursor")
		}
	}
}

func toTopoTabletType(tabletType psdbconnect.TabletType) topodata.TabletType {
	switch tabletType {
	case psdbconnect.TabletType_replica:
		return topodata.TabletType_REPLICA
	case psdbconnect.TabletType_batch:
		return topodata.TabletType_RDONLY
	case psdbconnect.TabletType_primary:
		return topodata.TabletType_PRIMARY
	default:
		// Fall back to replica
		return topodata.TabletType_REPLICA
	}
}

func (p PlanetScaleEdgeDatabase) initializeVTGateClient(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, grpcclient.ConnPool, error) {
	if p.vtgateClientFn == nil {
		conn, err := grpcclient.Dial(ctx, ps.Host,
			clientoptions.WithDefaultTLSConfig(),
			clientoptions.WithCompression(true),
			clientoptions.WithConnectionPool(1),
			clientoptions.WithExtraCallOption(
				auth.NewBasicAuth(ps.Username, ps.Password).CallOption(),
			),
		)
		if err != nil {
			return nil, nil, err
		}
		return vtgateservice.NewVitessClient(conn), conn, err
	} else {
		vtgateClient, err := p.vtgateClientFn(ctx, ps)
		if err != nil {
			return nil, nil, err
		}
		return vtgateClient, nil, nil
	}
}

// printQueryResult will pretty-print an AirbyteRecordMessage to the logger.
// Copied from vtctl/query.go
func (p PlanetScaleEdgeDatabase) printQueryResult(qr *sqltypes.Result, tableNamespace, tableName string, ps *PlanetScaleSource) {
	data := QueryResultToRecords(qr, ps)

	for _, record := range data {
		p.Logger.Record(tableNamespace, tableName, record)
	}
}

func buildVStreamRequest(tabletType psdbconnect.TabletType, table string, shard string, keyspace string, gtid string, lastKnownPk *query.QueryResult) *vtgate.VStreamRequest {
	req := &vtgate.VStreamRequest{
		TabletType: toTopoTabletType(tabletType),
		Vgtid: &binlogdata.VGtid{
			ShardGtids: []*binlogdata.ShardGtid{
				{
					Shard:    shard,
					Keyspace: keyspace,
					Gtid:     gtid,
				},
			},
		},
		Flags: &vtgate.VStreamFlags{
			MinimizeSkew: true,
			Cells:        "planetscale_operator_default",
		},
		Filter: &binlogdata.Filter{
			Rules: []*binlogdata.Rule{{
				Match:  table,
				Filter: "SELECT * FROM " + sqlescape.EscapeID(table),
			}},
		},
	}

	if lastKnownPk != nil {
		req.Vgtid.ShardGtids[0].TablePKs = []*binlogdata.TableLastPK{
			{
				TableName: table,
				Lastpk:    lastKnownPk,
			},
		}
	}

	return req
}

// positionEqual returns true if position `a` is equal to or after position `b`
func positionEqual(a string, b string) bool {
	if a == "" || b == "" {
		return false
	}

	parsedA, err := vtmysql.DecodePosition(a)
	if err != nil {
		return false
	}

	parsedB, err := vtmysql.DecodePosition(b)
	if err != nil {
		return false
	}

	return parsedA.Equal(parsedB)
}

// positionAfter returns true if position `a` is after position `b`
func positionAfter(a string, b string) bool {
	if a == "" || b == "" {
		return false
	}

	parsedA, err := vtmysql.DecodePosition(a)
	if err != nil {
		return false
	}

	parsedB, err := vtmysql.DecodePosition(b)
	if err != nil {
		return false
	}

	return !parsedA.Equal(parsedB) && parsedA.AtLeast(parsedB)
}
