package internal

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/edge-gateway/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/psdb/auth"
	grpcclient "github.com/planetscale/psdb/core/pool"
	clientoptions "github.com/planetscale/psdb/core/pool/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"vitess.io/vitess/go/sqltypes"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

type PlanetScaleEdgeDatabase struct {
	Logger AirbyteLogger
}

func (p PlanetScaleEdgeDatabase) CanConnect(ctx context.Context, psc PlanetScaleConnection) (bool, error) {
	var db *sql.DB
	db, err := sql.Open("mysql", psc.DSN(psc.TabletType))
	if err != nil {
		return false, err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (p PlanetScaleEdgeDatabase) HasTabletType(ctx context.Context, psc PlanetScaleConnection, tt psdbconnect.TabletType) (bool, error) {

	if p.supportsTabletType(ctx, psc, tt) {
		return true, nil
	}

	return false, errors.Errorf("Does not support tablet type : [%v]", TabletTypeToString(tt))
}

func (p PlanetScaleEdgeDatabase) DiscoverSchema(ctx context.Context, psc PlanetScaleConnection) (Catalog, error) {
	var c Catalog
	db, err := sql.Open("mysql", psc.DSN(psc.TabletType))
	if err != nil {
		return c, errors.Wrap(err, "Unable to open SQL connection")
	}
	defer db.Close()

	tableNamesQR, err := db.Query(fmt.Sprintf("show tables from `%s`;", psc.Database))
	if err != nil {
		return c, errors.Wrap(err, "Unable to query database for schema")
	}

	var tables []string

	for tableNamesQR.Next() {
		var name string
		if err = tableNamesQR.Scan(&name); err != nil {
			return c, errors.Wrap(err, "unable to get table names")
		}

		tables = append(tables, name)
	}
	if err := tableNamesQR.Err(); err != nil {
		return c, errors.Wrap(err, "unable to iterate table rows")
	}

	for _, tableName := range tables {
		stream, err := getStreamForTable(ctx, tableName, psc.Database, db)
		if err != nil {
			return c, errors.Wrapf(err, "unable to get stream for table %v", tableName)
		}
		c.Streams = append(c.Streams, stream)
	}
	return c, nil
}

func getStreamForTable(ctx context.Context, tableName, keyspace string, db *sql.DB) (Stream, error) {
	schema := StreamSchema{
		Type:       "object",
		Properties: map[string]PropertyType{},
	}
	stream := Stream{
		Name:               tableName,
		Schema:             schema,
		SupportedSyncModes: []string{"full_refresh", "incremental"},
		Namespace:          keyspace,
	}

	columnNamesQR, err := db.QueryContext(
		ctx,
		"select column_name, column_type from information_schema.columns where table_name=? AND table_schema=?;",
		tableName, keyspace,
	)
	if err != nil {
		return stream, errors.Wrapf(err, "Unable to get column names & types for table %v", tableName)
	}

	for columnNamesQR.Next() {
		var (
			name       string
			columnType string
		)
		if err = columnNamesQR.Scan(&name, &columnType); err != nil {
			return stream, errors.Wrapf(err, "Unable to scan row for column names & types of table %v", tableName)
		}

		stream.Schema.Properties[name] = PropertyType{getJsonSchemaType(columnType)}
	}
	if err := columnNamesQR.Err(); err != nil {
		return stream, errors.Wrapf(err, "unable to iterate column names and tables for table %s", tableName)
	}

	// need this otherwise airbyte will fail schema discovery for views
	// without primary keys.
	stream.PrimaryKeys = [][]string{}
	primaryKeysQR, err := db.QueryContext(
		ctx,
		"select column_name from information_schema.columns where table_schema=? AND table_name=? AND column_key='PRI';",
		keyspace, tableName,
	)
	if err != nil {
		return stream, errors.Wrapf(err, "Unable to get primary key column names for table %v", tableName)
	}

	for primaryKeysQR.Next() {
		var name string
		if err = primaryKeysQR.Scan(&name); err != nil {
			return stream, errors.Wrapf(err, "Unable to scan row for primary keys of table %v", tableName)
		}

		stream.PrimaryKeys = append(stream.PrimaryKeys, []string{name})
		stream.DefaultCursorFields = append(stream.DefaultCursorFields, name)
	}
	if err := primaryKeysQR.Err(); err != nil {
		return stream, errors.Wrapf(err, "unable to iterate primary keys for table %s", tableName)
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

func (p PlanetScaleEdgeDatabase) ListShards(ctx context.Context, psc PlanetScaleConnection) ([]string, error) {
	var shards []string
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", psc.DSN(psc.TabletType))
	if err != nil {
		return shards, errors.Wrap(err, "Unable to open SQL connection")
	}
	defer db.Close()

	// TODO: is there a prepared statement equivalent?
	shardNamesQR, err := db.QueryContext(
		ctx,
		`show vitess_shards like "%`+psc.Database+`%";`,
	)
	if err != nil {
		return shards, errors.Wrap(err, "Unable to query database for shards")
	}

	for shardNamesQR.Next() {
		var name string
		if err = shardNamesQR.Scan(&name); err != nil {
			return shards, errors.Wrap(err, "unable to get shard names")
		}

		shards = append(shards, strings.TrimPrefix(name, psc.Database+"/"))
	}
	if err := shardNamesQR.Err(); err != nil {
		return shards, errors.Wrapf(err, "unable to iterate shard names for %s", psc.Database)
	}

	return shards, nil
}

func (p PlanetScaleEdgeDatabase) Read(ctx context.Context, w io.Writer, ps PlanetScaleConnection, s ConfiguredStream, tc *psdbconnect.TableCursor) (*SerializedCursor, error) {
	var (
		err     error
		sc      *SerializedCursor
		cancel  context.CancelFunc
		hasRows bool
	)

	p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("picking tablet type : [%s]", strings.ToUpper(TabletTypeToString(ps.TabletType))))
	if ps.TabletType == psdbconnect.TabletType_primary {
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

		hasRows, _, _ = p.sync(peekCtx, tc, table, ps, true)
		if !hasRows {
			p.Logger.Log(LOGLEVEL_INFO, "no new rows found, exiting")
			return TableCursorToSerializedCursor(tc)
		}
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("new rows found, syncing rows for %v", readDuration))

		_, tc, err = p.sync(readCtx, tc, table, ps, false)
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

func (p PlanetScaleEdgeDatabase) sync(ctx context.Context, tc *psdbconnect.TableCursor, s Stream, ps PlanetScaleConnection, peek bool) (bool, *psdbconnect.TableCursor, error) {
	defer p.Logger.Flush()
	var err error

	conn, err := grpcclient.Dial(context.Background(), ps.Host,
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

	if tc.LastKnownPk != nil {
		tc.Position = ""
	}

	sReq := &psdbconnect.SyncRequest{
		TableName: s.Name,
		Cursor:    tc,
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

func (p PlanetScaleEdgeDatabase) supportsTabletType(ctx context.Context, psc PlanetScaleConnection, tt psdbconnect.TabletType) bool {
	canConnect, err := p.CanConnect(ctx, psc)
	if err != nil || !canConnect {
		return false
	}

	db, err := sql.Open("mysql", psc.DSN(tt))
	if err != nil {
		return false
	}
	defer db.Close()
	tabletsQR, err := db.QueryContext(ctx, "Show vitess_tablets")
	if err != nil {
		return false
	}

	for tabletsQR.Next() {
		var (
			cell                 string
			keyspace             string
			shard                string
			tabletType           string
			state                string
			alias                string
			hostname             string
			primaryTermStartTime string
		)
		// output is of the form :
		//aws_useast1c_5 connect-test - PRIMARY SERVING aws_useast1c_5-2797914161 10.200.131.217 2022-05-09T14:11:56Z
		//aws_useast1c_5 connect-test - REPLICA SERVING aws_useast1c_5-1559247072 10.200.178.136
		//aws_useast1c_5 connect-test - PRIMARY SERVING aws_useast1c_5-2797914161 10.200.131.217 2022-05-09T14:11:56Z
		//aws_useast1c_5 connect-test - REPLICA SERVING aws_useast1c_5-1559247072 10.200.178.136
		err := tabletsQR.Scan(&cell, &keyspace, &shard, &tabletType, &state, &alias, &hostname, &primaryTermStartTime)
		if err != nil {
			return false
		}

		if strings.EqualFold(tabletType, TabletTypeToString(tt)) && strings.EqualFold(state, "SERVING") {
			return true
		}
	}

	return false
}
