package internal

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
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
	Logger       AirbyteLogger
	Mysql        PlanetScaleEdgeMysqlAccess
	clientFn     func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error)
	readDuration time.Duration
}

func NewPlanetScaleEdgeDatabase(log AirbyteLogger, mysql PlanetScaleEdgeMysqlAccess) PlanetScaleDatabase {
	return PlanetScaleEdgeDatabase{
		Logger:       log,
		Mysql:        mysql,
		readDuration: 1 * time.Minute,
	}
}

func (p PlanetScaleEdgeDatabase) CanConnect(ctx context.Context, psc PlanetScaleSource) error {
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
func getJsonSchemaType(mysqlType string) PropertyType {
	// Support custom airbyte types documented here :
	// https://docs.airbyte.com/understanding-airbyte/supported-data-types/#the-types
	if strings.HasPrefix(mysqlType, "int") {
		return PropertyType{Type: "integer"}
	}

	if strings.HasPrefix(mysqlType, "bigint") {
		return PropertyType{Type: "string", AirbyteType: "big_integer"}
	}

	if strings.HasPrefix(mysqlType, "datetime") {
		return PropertyType{Type: "string", AirbyteType: "timestamp_with_timezone"}
	}

	switch mysqlType {
	case "tinyint(1)":
		return PropertyType{Type: "boolean"}
	case "date":
		return PropertyType{Type: "string", AirbyteType: "date"}
	case "datetime":
		return PropertyType{Type: "string", AirbyteType: "timestamp_with_timezone"}
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

func (p PlanetScaleEdgeDatabase) Read(ctx context.Context, w io.Writer, ps PlanetScaleSource, s ConfiguredStream, tc *psdbconnect.TableCursor) (*SerializedCursor, error) {
	var (
		sc           *SerializedCursor
		syncErr      error
		serializeErr error
	)

	tabletType := psdbconnect.TabletType_primary
	table := s.Stream

	for {
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("syncing rows for stream [%v] in namespace [%v] with cursor [%v]", table.Name, table.Namespace, tc))
		tc, syncErr = p.sync(ctx, tc, table, ps, tabletType)
		if tc != nil {
			sc, serializeErr = TableCursorToSerializedCursor(tc)
			if serializeErr != nil {
				return sc, serializeErr
			}
		}

		if syncErr != nil {
			if s, ok := status.FromError(syncErr); ok {
				if s.Code() == codes.DeadlineExceeded {
					p.Logger.Log(LOGLEVEL_INFO, "Continuing with cursor after server timeout")
				} else {
					// if the error is anything other than server timeout, keep going
					p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("Got error [%v], Returning with cursor :[%v]", s.Code(), tc))
					return sc, nil
				}
			} else if errors.Is(syncErr, io.EOF) {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("Done synchronizing rows for stream [%v]", table.Name))
				return sc, nil
			} else {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("non-grpc error [%v]]", syncErr))
				return sc, syncErr
			}
		}
	}
}

func (p PlanetScaleEdgeDatabase) sync(ctx context.Context, tc *psdbconnect.TableCursor, s Stream, ps PlanetScaleSource, tabletType psdbconnect.TabletType) (*psdbconnect.TableCursor, error) {
	defer p.Logger.Flush()
	ctx, cancel := context.WithTimeout(ctx, p.readDuration)
	defer cancel()

	var (
		err    error
		client psdbconnect.ConnectClient
	)

	if p.clientFn == nil {
		conn, err := grpcclient.Dial(ctx, ps.Host,
			clientoptions.WithDefaultTLSConfig(),
			clientoptions.WithCompression(true),
			clientoptions.WithConnectionPool(1),
			clientoptions.WithExtraCallOption(
				auth.NewBasicAuth(ps.Username, ps.Password).CallOption(),
			),
		)
		if err != nil {
			return tc, err
		}

		client = psdbconnect.NewConnectClient(conn)
	} else {
		client, err = p.clientFn(ctx, ps)
		if err != nil {
			return tc, err
		}
	}

	if tc.LastKnownPk != nil {
		tc.Position = ""
	}

	p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("Syncing with cursor position : [%v], last known PK : %v", tc.Position, tc.LastKnownPk != nil))

	sReq := &psdbconnect.SyncRequest{
		TableName:  s.Name,
		Cursor:     tc,
		TabletType: tabletType,
	}

	c, err := client.Sync(ctx, sReq)
	if err != nil {
		return tc, err
	}

	keyspaceOrDatabase := s.Namespace
	if keyspaceOrDatabase == "" {
		keyspaceOrDatabase = ps.Database
	}
	for {
		res, err := c.Recv()
		if err != nil {
			return tc, err
		}

		if res.Cursor != nil {
			// print the cursor to stdout here.
			tc = res.Cursor
		}

		if len(res.Result) > 0 {
			for _, result := range res.Result {
				qr := sqltypes.Proto3ToResult(result)
				for _, row := range qr.Rows {
					sqlResult := &sqltypes.Result{
						Fields: result.Fields,
					}
					sqlResult.Rows = append(sqlResult.Rows, row)
					// print AirbyteRecord messages to stdout here.
					p.printQueryResult(sqlResult, keyspaceOrDatabase, s.Name)
				}
			}
		}
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
