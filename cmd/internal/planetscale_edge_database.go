package internal

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/connect/source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/psdb/auth"
	grpcclient "github.com/planetscale/psdb/core/pool"
	clientoptions "github.com/planetscale/psdb/core/pool/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"vitess.io/vitess/go/sqltypes"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

// PlanetScaleDatabase is a general purpose interface
// that defines all the data access methods needed for the PlanetScale Airbyte source to function.
type PlanetScaleDatabase interface {
	CanConnect(ctx context.Context, ps PlanetScaleSource) (bool, error)
	DiscoverSchema(ctx context.Context, ps PlanetScaleSource) (Catalog, error)
	ListShards(ctx context.Context, ps PlanetScaleSource) ([]string, error)
	Read(ctx context.Context, w io.Writer, ps PlanetScaleSource, s ConfiguredStream, tc *psdbconnect.TableCursor) (*SerializedCursor, error)
}

// PlanetScaleEdgeDatabase is an implementation of the PlanetScaleDatabase interface defined above.
// It uses the mysql interface provided by PlanetScale for all schema/shard/tablet discovery and
// the grpc API for incrementally syncing rows from PlanetScale.
type PlanetScaleEdgeDatabase struct {
	Logger   AirbyteLogger
	Mysql    PlanetScaleEdgeMysqlAccess
	clientFn func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error)
}

func (p PlanetScaleEdgeDatabase) CanConnect(ctx context.Context, psc PlanetScaleSource) (bool, error) {
	return p.Mysql.PingContext(ctx, psc)
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
		stream.DefaultCursorFields = append(stream.DefaultCursorFields, key)
	}

	stream.SourceDefinedCursor = true
	return stream, nil
}

// Convert columnType to Airbyte type.
func getJsonSchemaType(mysqlType string) string {
	if strings.HasPrefix(mysqlType, "int") {
		return "integer"
	}

	if mysqlType == "tinyint(1)" {
		return "boolean"
	}

	return "string"
}

func (p PlanetScaleEdgeDatabase) ListShards(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	return p.Mysql.GetVitessShards(ctx, psc)
}

func (p PlanetScaleEdgeDatabase) Read(ctx context.Context, w io.Writer, ps PlanetScaleSource, s ConfiguredStream, tc *psdbconnect.TableCursor) (*SerializedCursor, error) {
	var (
		err     error
		sc      *SerializedCursor
		cancel  context.CancelFunc
		hasRows bool
	)

	tabletType := psdbconnect.TabletType_primary

	if tc.Shard == "-" {
		// TODO : fix https://github.com/planetscale/issues/issues/296
		// We make all non default keyspaces connect to the PRIMARY.
		if p.supportsTabletType(ctx, ps, "", psdbconnect.TabletType_replica) {
			tabletType = psdbconnect.TabletType_replica
		}
	}

	p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("picking tablet type : [%s]", strings.ToUpper(TabletTypeToString(tabletType))))
	if tabletType == psdbconnect.TabletType_primary {
		p.Logger.Log(LOGLEVEL_WARN, "Connecting to the primary to download data might cause performance issues with your database")
	}

	table := s.Stream
	readDuration := 1 * time.Minute
	peekCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	readCtx, readCancel := context.WithTimeout(ctx, readDuration)
	defer readCancel()

	for {
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("syncing rows for stream [%v] in namespace [%v] with cursor [%v]", table.Name, table.Namespace, tc))
		p.Logger.Log(LOGLEVEL_INFO, "peeking to see if there's any new rows")

		hasRows, _, _ = p.sync(peekCtx, tc, table, ps, tabletType, true)
		if !hasRows {
			p.Logger.Log(LOGLEVEL_INFO, "no new rows found, exiting")
			return TableCursorToSerializedCursor(tc)
		}
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("new rows found, syncing rows for %v", readDuration))

		_, tc, err = p.sync(readCtx, tc, table, ps, tabletType, false)
		if tc != nil {
			sc, err = TableCursorToSerializedCursor(tc)
			if err != nil {
				return sc, err
			}
		}
		if err != nil {
			if s, ok := status.FromError(err); ok {
				// if the error is anything other than server timeout, keep going
				if s.Code() != codes.DeadlineExceeded {
					p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("Got error [%v], Returning with cursor :[%v] after server timeout", s.Code(), tc))
					return sc, nil
				} else {
					p.Logger.Log(LOGLEVEL_INFO, "Continuing with cursor after server timeout")
				}
			} else {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("non-grpc error [%v]]", err))
				return sc, err
			}
		}
	}
}
func (p PlanetScaleEdgeDatabase) client(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
	if p.clientFn != nil {
		return p.clientFn(ctx, ps)
	}
	conn, err := grpcclient.Dial(ctx, ps.Host,
		clientoptions.WithDefaultTLSConfig(),
		clientoptions.WithCompression(true),
		clientoptions.WithConnectionPool(1),
		clientoptions.WithExtraCallOption(
			auth.NewBasicAuth(ps.Username, ps.Password).CallOption(),
		),
	)
	if err != nil {
		panic(err)
	}

	client := psdbconnect.NewConnectClient(conn)
	return client, nil
}
func (p PlanetScaleEdgeDatabase) sync(ctx context.Context, tc *psdbconnect.TableCursor, s Stream, ps PlanetScaleSource, tabletType psdbconnect.TabletType, peek bool) (bool, *psdbconnect.TableCursor, error) {
	defer p.Logger.Flush()

	var err error
	client, err := p.client(ctx, ps)
	if err != nil {
		panic(err)
	}

	if tc.LastKnownPk != nil {
		tc.Position = ""
	}

	sReq := &psdbconnect.SyncRequest{
		TableName:  s.Name,
		Cursor:     tc,
		TabletType: tabletType,
	}

	c, err := client.Sync(ctx, sReq)
	if err != nil {
		return false, tc, nil
	}
	keyspaceOrDatabase := s.Namespace
	if keyspaceOrDatabase == "" {
		keyspaceOrDatabase = ps.Database
	}
	for {
		res, err := c.Recv()
		if errors.Is(err, io.EOF) {
			// we're done receiving rows from the server
			return false, tc, nil
		}
		if err != nil {
			return false, tc, err
		}

		if res.Cursor != nil {
			// print the cursor to stdout here.
			tc = res.Cursor
			if peek {
				return true, nil, nil
			}
		}

		if len(res.Result) > 0 {
			if peek {
				return true, nil, nil
			}
			for _, result := range res.Result {
				qr := sqltypes.Proto3ToResult(result)
				sqlResult := &sqltypes.Result{
					Fields: result.Fields,
				}
				sqlResult.Rows = append(sqlResult.Rows, qr.Rows[0])
				// print AirbyteRecord messages to stdout here.
				p.printQueryResult(sqlResult, keyspaceOrDatabase, s.Name)
			}
		}
	}
}

// printQueryResult will pretty-print an AirbyteRecordMessage to the logger.
// Copied from vtctl/query.go
func (p PlanetScaleEdgeDatabase) printQueryResult(qr *sqltypes.Result, tableNamespace, tableName string) {
	data := QueryResultToRecords(qr, false)

	for _, record := range data {
		p.Logger.Record(tableNamespace, tableName, record)
	}
}

func (p PlanetScaleEdgeDatabase) supportsTabletType(ctx context.Context, psc PlanetScaleSource, shardName string, tt psdbconnect.TabletType) bool {
	canConnect, err := p.CanConnect(ctx, psc)
	if err != nil || !canConnect {
		return false
	}

	tablets, err := p.Mysql.GetVitessTablets(ctx, psc)
	if err != nil {
		return false
	}

	for _, tablet := range tablets {
		keyspaceHasTablet := strings.EqualFold(tablet.Keyspace, psc.Database)
		tabletTypeIsServing := keyspaceHasTablet && strings.EqualFold(tablet.TabletType, TabletTypeToString(tt)) && strings.EqualFold(tablet.State, "SERVING")
		matchesShardName := true

		if shardName != "" {
			matchesShardName = strings.EqualFold(tablet.Shard, shardName)
		}

		if keyspaceHasTablet && tabletTypeIsServing && matchesShardName {
			return true
		}
	}

	return false
}
