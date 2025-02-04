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
	vtgateservice "vitess.io/vitess/go/vt/proto/vtgateservice"
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
func getJsonSchemaType(mysqlType string, treatTinyIntAsBoolean bool) PropertyType {
	// Support custom airbyte types documented here :
	// https://docs.airbyte.com/understanding-airbyte/supported-data-types/#the-types
	switch {
	case strings.HasPrefix(mysqlType, "tinyint(1)"):
		if treatTinyIntAsBoolean {
			return PropertyType{Type: "boolean"}
		}
		return PropertyType{Type: "number", AirbyteType: "integer"}
	case strings.HasPrefix(mysqlType, "int"), strings.HasPrefix(mysqlType, "smallint"), strings.HasPrefix(mysqlType, "mediumint"), strings.HasPrefix(mysqlType, "bigint"), strings.HasPrefix(mysqlType, "tinyint"):
		return PropertyType{Type: "number", AirbyteType: "integer"}
	case strings.HasPrefix(mysqlType, "decimal"), strings.HasPrefix(mysqlType, "double"), strings.HasPrefix(mysqlType, "float"):
		return PropertyType{Type: "number"}
	case strings.HasPrefix(mysqlType, "datetime"), strings.HasPrefix(mysqlType, "timestamp"):
		return PropertyType{Type: "string", CustomFormat: "date-time", AirbyteType: "timestamp_without_timezone"}
	case strings.HasPrefix(mysqlType, "date"):
		return PropertyType{Type: "string", CustomFormat: "date", AirbyteType: "date"}
	case strings.HasPrefix(mysqlType, "time"):
		return PropertyType{Type: "string", CustomFormat: "time", AirbyteType: "time_without_timezone"}
	default:
		return PropertyType{Type: "string"}
	}
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
	)

	tabletType := psdbconnect.TabletType_primary
	if ps.UseRdonly {
		tabletType = psdbconnect.TabletType_batch
	} else if ps.UseReplica {
		tabletType = psdbconnect.TabletType_replica
	}

	currentPosition := lastKnownPosition
	table := s.Stream
	readDuration := 1 * time.Minute
	preamble := fmt.Sprintf("[%v:%v:%v shard : %v] ", table.Namespace, TabletTypeToString(tabletType), table.Name, currentPosition.Shard)

	for {
		p.Logger.Log(LOGLEVEL_INFO, preamble+"Peeking to see if there's any new rows")
		latestCursorPosition, lcErr := p.getLatestCursorPosition(ctx, currentPosition.Shard, currentPosition.Keyspace, table, ps, tabletType)
		if lcErr != nil {
			return currentSerializedCursor, errors.Wrap(err, "Unable to get latest cursor position")
		}
		if latestCursorPosition == "" {
			return currentSerializedCursor, errors.Wrap(err, "Unable to get latest cursor position")
		}

		// the last synced VGTID is not at least, or after the current VGTID
		if currentPosition.Position != "" && !positionAtLeast(latestCursorPosition, currentPosition.Position) {
			p.Logger.Log(LOGLEVEL_INFO, preamble+"No new rows found, exiting")
			return TableCursorToSerializedCursor(currentPosition)
		}
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf(preamble+"New rows found, syncing rows for %v", readDuration))
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf(preamble+"Syncing rows from cursor [%v]", currentPosition))

		currentPosition, recordCount, err := p.sync(ctx, currentPosition, latestCursorPosition, table, ps, tabletType, readDuration)
		if currentPosition.Position != "" {
			currentSerializedCursor, sErr = TableCursorToSerializedCursor(currentPosition)
			if sErr != nil {
				// if we failed to serialize here, we should bail.
				return currentSerializedCursor, errors.Wrap(sErr, "unable to serialize current position")
			}
		}
		if err != nil {
			if s, ok := status.FromError(err); ok {
				// if the error is anything other than server timeout, keep going
				if s.Code() != codes.DeadlineExceeded {
					p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%vGot error [%v], returning with cursor [%v] after server timeout", preamble, s.Code(), currentPosition))
					return currentSerializedCursor, nil
				} else {
					p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%v%v records synced. Continuing with cursor after recoverable error %+v", preamble, recordCount, err))
				}
			} else if errors.Is(err, io.EOF) {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%vFinished reading %v records for table [%v]", preamble, recordCount, table.Name))
				return currentSerializedCursor, nil
			} else {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%vNon-grpc error [%v]]", preamble, err))
				return currentSerializedCursor, err
			}
		}
	}
}

func (p PlanetScaleEdgeDatabase) sync(ctx context.Context, tc *psdbconnect.TableCursor, stopPosition string, s Stream, ps PlanetScaleSource, tabletType psdbconnect.TabletType, readDuration time.Duration) (*psdbconnect.TableCursor, int, error) {
	preamble := fmt.Sprintf("[%v:%v:%v shard : %v] ", s.Namespace, TabletTypeToString(tabletType), s.Name, tc.Shard)

	defer p.Logger.Flush()
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

	// If there is a LastKnownPk, that means we were in a copy phase
	// Setting position to "" specifies COPY phase in the VStream API
	if tc.LastKnownPk != nil {
		tc.Position = ""
	}

	waitForCopyCompleted := tc.Position == ""
	p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sWill be waiting for COPY_COMPLETED event? %v", preamble, waitForCopyCompleted))

	vtgateReq := buildVStreamRequest(tabletType, s.Name, tc.Shard, tc.Keyspace, tc.Position)
	p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sRequesting to sync from cursor position [%v] to stop cursor position [%v] in cells %v; using last known PK: %v", preamble, tc.Position, stopPosition, vtgateReq.Flags.Cells, tc.LastKnownPk != nil))
	c, err := vtgateClient.VStream(ctx, vtgateReq)

	if err != nil {
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sExiting sync due to client sync error: %+v", preamble, err))
		return tc, 0, err
	}

	keyspaceOrDatabase := s.Namespace
	if keyspaceOrDatabase == "" {
		keyspaceOrDatabase = ps.Database
	}

	// Stop when we've reached or surpassed the stop position for this sync session
	watchForVgGtidChange := false
	resultCount := 0
	loopCount := 0

	var fields []*query.Field

	for {
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sStarting sync loop #%v", preamble, loopCount))
		loopCount += 1

		res, err := c.Recv()
		if err != nil {
			p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sExiting sync and flushing records due to error: %+v", preamble, err))
			return tc, resultCount, err
		}

		var rows []*query.QueryResult

		for _, event := range res.Events {
			switch event.Type {
			case binlogdata.VEventType_VGTID:
				vgtid := event.GetVgtid().ShardGtids[0]
				if vgtid != nil {
					// Update cursor to new VGTID
					tc = &psdbconnect.TableCursor{
						Shard:    event.Shard,
						Keyspace: event.Keyspace,
						Position: vgtid.Gtid,
					}
					p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sVGTID event found, advancing cursor to %+v", preamble, tc))
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
				waitForCopyCompleted = false
			}
		}

		if len(rows) > 0 {
			p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sROW event found, with %+v rows", preamble, len(rows)))
			// Watch for VGTID change as soon as we encounter records from some VGTID that is equal to, or after the stop position we're looking for.
			// We watch for a VGTID that is equal to or after (not just equal to) the stop position, because by the time the first sync for records occurs,
			// the current VGTID may have already advanced past the stop position.
			watchForVgGtidChange = watchForVgGtidChange || positionAtLeast(tc.Position, stopPosition)
			if watchForVgGtidChange {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sRows up to stop position [%+v] found, waiting for next VGTID. Waiting for COPY_COMPLETED event? %v", preamble, stopPosition, waitForCopyCompleted))
			}
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

		// Exit sync and flush records once the VGTID position is past the desired stop position, and we're no longer waiting for COPY phase to complete
		if watchForVgGtidChange && positionAfter(tc.Position, stopPosition) && !waitForCopyCompleted {
			p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sExiting sync and flushing records because current position %+v has passed stop position %+v", preamble, tc.Position, stopPosition))
			return tc, resultCount, io.EOF
		}
	}
}

func (p PlanetScaleEdgeDatabase) getLatestCursorPosition(ctx context.Context, shard, keyspace string, s Stream, ps PlanetScaleSource, tabletType psdbconnect.TabletType) (string, error) {
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

	vtgateReq := buildVStreamRequest(tabletType, s.Name, shard, keyspace, "current")
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

func buildVStreamRequest(tabletType psdbconnect.TabletType, table string, shard string, keyspace string, gtid string) *vtgate.VStreamRequest {
	return &vtgate.VStreamRequest{
		TabletType: topodata.TabletType(tabletType),
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
}

// positionAtLeast returns true if position `a` is equal to or after position `b`
func positionAtLeast(a string, b string) bool {
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

	return parsedA.AtLeast(parsedB)
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
