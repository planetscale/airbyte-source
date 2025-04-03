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

	tc := lastKnownPosition
	if tc.Position == "" || tc.LastKnownPk != nil {
		syncMode = "full"
	} else {
		syncMode = "incremental"
	}

	table := s.Stream
	readDuration := 5 * time.Minute
	maxRetries := ps.MaxRetries

	preamble := fmt.Sprintf("[%v:%v:%v shard : %v] ", table.Namespace, TabletTypeToString(tabletType), table.Name, tc.Shard)

	p.Logger.Log(LOGLEVEL_INFO, preamble+"Peeking to see if there's any new GTIDs")
	stopPosition, lcErr := p.getStopCursorPosition(ctx, tc.Shard, tc.Keyspace, table, ps, tabletType)
	if lcErr != nil {
		p.Logger.Log(LOGLEVEL_ERROR, preamble+fmt.Sprintf("Error fetching latest cursor position: %+v", lcErr))
		return currentSerializedCursor, errors.Wrap(err, "Unable to get latest cursor position")
	}
	if stopPosition == "" {
		p.Logger.Log(LOGLEVEL_ERROR, preamble+fmt.Sprintf("Error fetching latest cursor position, was empty string: %+v", stopPosition))
		return currentSerializedCursor, errors.Wrap(err, "Unable to get latest cursor position")
	}

	// the last synced VGTID is not after the current VGTID
	if tc.Position != "" && !positionAfter(stopPosition, tc.Position) {
		p.Logger.Log(LOGLEVEL_INFO, preamble+"No new GTIDs found, exiting")
		return TableCursorToSerializedCursor(tc)
	}
	p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf(preamble+"New GTIDs found, syncing for %v", readDuration))

	var syncCount uint = 0
	totalRecordCount := 0

	for {
		syncCount += 1
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sStarting sync #%v", preamble, syncCount))
		newTC, recordCount, err := p.sync(
			ctx, syncMode, tc, stopPosition, table, ps, tabletType, readDuration,
		)
		totalRecordCount += recordCount
		currentSerializedCursor, sErr = TableCursorToSerializedCursor(tc)
		if sErr != nil {
			// if we failed to serialize here, we should bail.
			return currentSerializedCursor, errors.Wrap(sErr, "unable to serialize current position")
		}
		if err != nil {
			if s, ok := status.FromError(err); ok {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%v%v records synced after %v syncs. Got error [%v], returning with cursor [%v] after gRPC error", preamble, totalRecordCount, syncCount, s.Code(), tc))
				if syncCount >= maxRetries {
					return currentSerializedCursor, nil
				}
			} else if errors.Is(err, io.EOF) {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%vFinished reading %v records after %v syncs for table [%v]", preamble, totalRecordCount, syncCount, table.Name))
				return currentSerializedCursor, nil
			} else {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%v%v records synced after %v syncs. Got error [%v], returning with cursor [%v] after server timeout", preamble, totalRecordCount, syncCount, err, tc))
				return currentSerializedCursor, err
			}
		}
		tc = newTC
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%vContinuing to next sync #%v. Set next sync start position to [%+v].", preamble, syncCount+1, tc))
	}
}

type syncState struct {
	copyCompletedSeen bool
	fields            []*query.Field
	lastKnownPk       *query.QueryResult
	position          string
	results           []*query.QueryResult
}

func (p PlanetScaleEdgeDatabase) sync(
	ctx context.Context, syncMode string,
	tc *psdbconnect.TableCursor,
	stopPosition string,
	s Stream,
	ps PlanetScaleSource,
	tabletType psdbconnect.TabletType,
	readDuration time.Duration,
) (*psdbconnect.TableCursor, int, error) {
	preamble := fmt.Sprintf("[%v:%v:%v shard : %v] ", s.Namespace, TabletTypeToString(tabletType), s.Name, tc.Shard)
	logf := func(level, message string, args ...any) {
		p.Logger.Log(level, fmt.Sprintf("%s%s", preamble, fmt.Sprintf(message, args...)))
	}

	ctx, cancel := context.WithTimeout(ctx, readDuration)
	defer cancel()

	var (
		err          error
		vtgateClient vtgateservice.VitessClient
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
	logf(LOGLEVEL_INFO, "Requesting VStream with %+v", vtgateReq)

	if isFullSync {
		logf(LOGLEVEL_INFO, "Will stop once COPY COMPLETED event is seen.")
	} else {
		logf(LOGLEVEL_INFO, "Will stop once stop position [%+v] is found.", stopPosition)
	}

	c, err := vtgateClient.VStream(ctx, vtgateReq)
	if err != nil {
		logf(LOGLEVEL_ERROR, "Exiting sync due to client sync error: %+v", err)
		return tc, 0, err
	}

	keyspaceOrDatabase := s.Namespace
	if keyspaceOrDatabase == "" {
		keyspaceOrDatabase = ps.Database
	}

	var (
		// Can finish sync once we've synced to the stop position, or finished the
		// VStream COPY phase
		done bool

		// Number of rows read from VStream.
		nread int

		// As the sync progresses, it updates its state.
		state = syncState{position: tc.Position}
	)

	for !done {
		vstreamResp, err := recvVstream(logf, c)
		if err != nil {
			return tc, nread, err
		}

		err = digestVStream(logf, vstreamResp.Events, &state)
		if err != nil {
			return tc, nread, err
		}

		if isFullSync && state.copyCompletedSeen {
			logf(LOGLEVEL_INFO, "Ready to finish sync and flush since copy phase completed or stop VGTID passed")
			done = true
		}

		if !isFullSync && positionEqual(state.position, stopPosition) {
			logf(LOGLEVEL_INFO, "Ready to finish sync and flush since stop position [%+v] found", stopPosition)
			done = true
		}

		for _, result := range state.results {
			qr := sqltypes.Proto3ToResult(result)
			for _, row := range qr.Rows {
				nread += 1
				data := QueryResultToRecords(&sqltypes.Result{
					Fields: result.Fields,
					Rows:   []sqltypes.Row{row},
				}, &ps)
				for _, record := range data {
					if p.Logger.QueueFull() {
						if err := checkpoint(p.Logger.Flush, tc, state); err != nil {
							return tc, nread, fmt.Errorf("checkpoint while reading records: %w", err)
						}
					}
					if err := p.Logger.Record(keyspaceOrDatabase, s.Name, record); err != nil {
						return tc, nread, fmt.Errorf("record: %w", err)
					}
				}
			}
		}
		state.results = state.results[:0]
	}

	// Exit sync and flush records once the VGTID position is at or past the
	// desired stop position, and we're no longer waiting for COPY phase to
	// complete
	if isFullSync {
		logf(LOGLEVEL_INFO,
			"Exiting full sync and flushing records because COPY_COMPLETED event was seen, current position is %+v, stop position is %+v",
			tc.Position, stopPosition)
	} else {
		logf(LOGLEVEL_INFO,
			"Exiting incremental sync and flushing records because current position %+v has reached or passed stop position %+v",
			tc.Position, stopPosition)
	}

	if err := checkpoint(p.Logger.Flush, tc, state); err != nil {
		return tc, nread, fmt.Errorf("checkpoint after all records read: %w", err)
	}

	return tc, nread, io.EOF
}

func checkpoint(
	flush func() error,
	tc *psdbconnect.TableCursor,
	state syncState,
) error {
	if err := flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	if state.position != "" {
		tc.Position = state.position
	}

	if state.lastKnownPk != nil {
		tc.LastKnownPk = state.lastKnownPk
	}

	return nil
}

func recvVstream(
	logf func(level, message string, args ...any),
	c vtgateservice.Vitess_VStreamClient,
) (*vtgate.VStreamResponse, error) {
	resp, err := c.Recv()
	if err != nil {
		s, ok := status.FromError(err)
		switch {
		case ok && s.Code() == codes.DeadlineExceeded:
			// No next VGTID found within deadline.
			logf(LOGLEVEL_ERROR, "no new VGTID found before deadline exceeded: %v", err)
		case errors.Is(err, io.EOF):
			// EOF is an acceptable error indicating VStream is finished.
			logf(LOGLEVEL_ERROR, "encountered EOF, possibly indicating end of VStream")
		}
	}
	return resp, err
}

func digestVStream(
	logf func(level, message string, args ...any),
	events []*binlogdata.VEvent,
	state *syncState,
) error {
	for _, event := range events {
		switch event.Type {
		case binlogdata.VEventType_VGTID:
			vgtid := event.GetVgtid().ShardGtids[0]
			if vgtid != nil {
				state.position = vgtid.Gtid
				if vgtid.TablePKs != nil {
					tablePK := vgtid.TablePKs[0]
					if tablePK != nil {
						// Setting LastKnownPk allows a COPY phase to pick up where it left off
						lastPK := tablePK.Lastpk
						state.lastKnownPk = lastPK
					} else {
						state.lastKnownPk = nil
					}
				} else {
					state.lastKnownPk = nil
				}
			}
		case binlogdata.VEventType_LASTPK:
			if event.LastPKEvent.TableLastPK != nil {
				// Only update last PK because we're in a COPY phase
				logf(LOGLEVEL_INFO, "LASTPK event found, setting last PK to %+v", event.LastPKEvent.TableLastPK.Lastpk)
				state.lastKnownPk = event.LastPKEvent.TableLastPK.Lastpk
			}
		case binlogdata.VEventType_FIELD:
			// Save fields for processing
			logf(LOGLEVEL_INFO, "FIELD event found, setting fields to %+v", event.FieldEvent.Fields)
			state.fields = event.FieldEvent.Fields
		case binlogdata.VEventType_ROW:
			// Collect rows for processing
			for _, change := range event.RowEvent.RowChanges {
				if change.After != nil {
					state.results = append(state.results, &query.QueryResult{
						Fields: state.fields,
						Rows:   []*query.Row{change.After},
					})
				}
			}
		case binlogdata.VEventType_COPY_COMPLETED:
			logf(LOGLEVEL_INFO, "COPY_COMPLETED event found, copy phase finished")
			state.copyCompletedSeen = true
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
			return fmt.Errorf("unexpected binlogdata.VEventType: %#v", event.Type)
		}
	}

	return nil
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
