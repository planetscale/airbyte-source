package internal

import (
	"context"
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
	Read(ctx context.Context, w io.Writer, ps PlanetScaleSource, s ConfiguredStream, tc *psdbconnect.TableCursor) (*SerializedCursor, error)
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

	stream.SourceDefinedCursor = true
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
		oneOf          []OneOfType
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
	case strings.HasPrefix(mysqlType, "datetime"), strings.HasPrefix(mysqlType, "timestamp"):
		jsonSchemaType = "string"
		customFormat = "date-time"
		airbyteType = "timestamp_without_timezone"
	case strings.HasPrefix(mysqlType, "date"):
		jsonSchemaType = "string"
		customFormat = "date"
		airbyteType = "date"
	case strings.HasPrefix(mysqlType, "time"):
		jsonSchemaType = "string"
		customFormat = "time"
		airbyteType = "time_without_timezone"
	default:
		jsonSchemaType = "string"
	}

	propertyType := PropertyType{
		Type:         &jsonSchemaType,
		CustomFormat: customFormat,
		AirbyteType:  airbyteType,
	}

	if strings.ToLower(nullable) == "yes" {
		oneOf = []OneOfType{
			{Type: "null"},
			{Type: jsonSchemaType},
		}

		propertyType.Type = nil
		propertyType.OneOf = oneOf
	}

	return propertyType
}

func (p PlanetScaleEdgeDatabase) Close() error {
	return p.Mysql.Close()
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
func (p PlanetScaleEdgeDatabase) Read(ctx context.Context, w io.Writer, ps PlanetScaleSource, s ConfiguredStream, lastKnownPosition *psdbconnect.TableCursor) (*SerializedCursor, error) {
	var (
		err                     error
		sErr                    error
		currentSerializedCursor *SerializedCursor
		syncMode                string
	)

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
	readDuration := 5 * time.Minute
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

	// the last synced VGTID is not at least, or after the current VGTID
	if currentPosition.Position != "" && !positionAfter(stopPosition, currentPosition.Position) {
		p.Logger.Log(LOGLEVEL_INFO, preamble+"No new GTIDs found, exiting")
		return TableCursorToSerializedCursor(currentPosition)
	}
	p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf(preamble+"New GTIDs found, syncing for %v", readDuration))

	var syncCount uint = 0
	totalRecordCount := 0

	for {
		syncCount += 1
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sStarting sync #%v", preamble, syncCount))
		newPosition, recordCount, err := p.sync(ctx, syncMode, currentPosition, stopPosition, table, ps, tabletType, readDuration)
		totalRecordCount += recordCount
		currentSerializedCursor, sErr = TableCursorToSerializedCursor(currentPosition)
		if sErr != nil {
			// if we failed to serialize here, we should bail.
			return currentSerializedCursor, errors.Wrap(sErr, "unable to serialize current position")
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

func (p PlanetScaleEdgeDatabase) sync(ctx context.Context, syncMode string, tc *psdbconnect.TableCursor, stopPosition string, s Stream, ps PlanetScaleSource, tabletType psdbconnect.TabletType, readDuration time.Duration) (*psdbconnect.TableCursor, int, error) {
	preamble := fmt.Sprintf("[%v:%v:%v shard : %v] ", s.Namespace, TabletTypeToString(tabletType), s.Name, tc.Shard)

	defer p.Logger.Flush()
	ctx, cancel := context.WithTimeout(ctx, readDuration)
	defer cancel()

	var (
		err          error
		vtgateClient vtgateservice.VitessClient
		fields       []*query.Field
	)

	vtgateClient, conn, err := p.initializeVTGateClient(ctx, ps)
	if err != nil {
		return tc, 0, err
	}
	if conn != nil {
		defer conn.Close()
	}

	if tc.LastKnownPk != nil {
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
		return tc, 0, err
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
				return tc, resultCount, err
			} else if err == io.EOF {
				// EOF is an acceptable error indicating VStream is finished.
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sExiting sync and flushing records because EOF encountered at position %+v", preamble, tc))
				return tc, resultCount, io.EOF
			} else {
				p.Logger.Log(LOGLEVEL_ERROR, fmt.Sprintf("%sExiting sync and flushing records due to error: %+v", preamble, err))
				return tc, resultCount, err
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
			}
		}

		// if isFullSync && copyCompletedSeen {
		if isFullSync && copyCompletedSeen {
			p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sReady to finish sync and flush since copy phase completed or stop VGTID passed", preamble))
			canFinishSync = true
		}
		if !isFullSync && positionEqual(tc.Position, stopPosition) {
			p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sReady to finish sync and flush since stop position [%+v] found", preamble, stopPosition))
			canFinishSync = true
		}

		// Exit sync and flush records once the VGTID position is past the desired stop position, and we're no longer waiting for COPY phase to complete
		if canFinishSync && positionAfter(tc.Position, stopPosition) {
			p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sExiting sync and flushing records because current position %+v has passed stop position %+v", preamble, tc.Position, stopPosition))
			return tc, resultCount, io.EOF
		}

		if len(rows) > 0 {
			for _, result := range rows {
				qr := sqltypes.Proto3ToResult(result)
				for _, row := range qr.Rows {
					resultCount += 1
					sqlResult := &sqltypes.Result{
						Fields: fields,
					}
					sqlResult.Rows = append(sqlResult.Rows, row)
					// Results queued to Airbyte here, and flushed at the end of sync()
					p.printQueryResult(sqlResult, keyspaceOrDatabase, s.Name)
				}
			}
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
	if tabletType == psdbconnect.TabletType_replica {
		return topodata.TabletType_REPLICA
	} else if tabletType == psdbconnect.TabletType_batch {
		return topodata.TabletType_RDONLY
	} else if tabletType == psdbconnect.TabletType_primary {
		return topodata.TabletType_PRIMARY
	}

	// Fall back to replica
	return topodata.TabletType_REPLICA
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
func (p PlanetScaleEdgeDatabase) printQueryResult(qr *sqltypes.Result, tableNamespace, tableName string) {
	data := QueryResultToRecords(qr)

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
