package internal

import (
	"context"
	"encoding/json"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/planetscale/edge-gateway/common/authorization"
	"github.com/planetscale/edge-gateway/gateway/router"
	psdbdatav1 "github.com/planetscale/edge-gateway/proto/psdb/data_v1"
	"github.com/planetscale/edge-gateway/psdbpool"
	"github.com/planetscale/edge-gateway/psdbpool/options"
	"vitess.io/vitess/go/sqltypes"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

type PlanetScaleEdgeDatabase struct {
	Logger AirbyteLogger
}

type SerializedCursor struct {
	Cursor string `json:"cursor"`
}

type SyncState struct {
	Cursors map[string]SerializedCursor
}

func (p PlanetScaleEdgeDatabase) CanConnect(ctx context.Context, psc PlanetScaleConnection) (bool, error) {
	return PlanetScaleMySQLDatabase{}.CanConnect(ctx, psc)
}

func (p PlanetScaleEdgeDatabase) DiscoverSchema(ctx context.Context, psc PlanetScaleConnection) (Catalog, error) {
	return PlanetScaleMySQLDatabase{}.DiscoverSchema(ctx, psc)
}

func (p PlanetScaleEdgeDatabase) Read(ctx context.Context, w io.Writer, ps PlanetScaleConnection, s Stream, tc *psdbdatav1.TableCursor) (*SerializedCursor, error) {
	var (
		sc  *SerializedCursor
		err error
	)

	syncTimeoutDuration := 2 * time.Second
	ctx, cancel := context.WithTimeout(ctx, syncTimeoutDuration)
	defer cancel()
	sc, err = p.sync(ctx, tc, s, ps)
	if err != nil {
		if s, ok := status.FromError(err); ok {
			if s.Code() == codes.DeadlineExceeded {
				return sc, nil
			}
		}
	}

	if sc == nil {
		p.Logger.Log(w, LOGLEVEL_INFO, "No new records returned, returning last known cursor")
		// if we didn't get a cursor in this sync operation, then there's no new records since the last cursor
		// resend the old cursor back.
		sc = p.serializeCursor(tc)
	} else {
		p.Logger.Log(w, LOGLEVEL_INFO, "new records returned, returning new cursor")
	}

	return sc, err
}

func (p PlanetScaleEdgeDatabase) sync(ctx context.Context, tc *psdbdatav1.TableCursor, s Stream, ps PlanetScaleConnection) (*SerializedCursor, error) {
	tlsConfig := options.DefaultTLSConfig()
	var sc *SerializedCursor
	var err error
	pool := psdbpool.New(
		router.NewSingleRoute(ps.Host),
		options.WithConnectionPool(4),
		options.WithTLSConfig(tlsConfig),
	)
	auth, err := authorization.NewBasicAuth(ps.Username, ps.Password)
	if err != nil {
		return sc, err
	}

	conn, err := pool.GetWithAuth(ctx, auth)
	if err != nil {
		return sc, err
	}
	defer conn.Release()

	sReq := &psdbdatav1.SyncRequest{
		TableName: s.Name,
		Cursor:    tc,
	}

	c, err := conn.Sync(ctx, sReq)
	if err != nil {
		return sc, nil
	}
	keyspaceOrDatabase := s.Namespace
	if keyspaceOrDatabase == "" {
		keyspaceOrDatabase = ps.Database
	}
	for {
		res, err := c.Recv()
		if errors.Is(err, io.EOF) {
			// we're done receiving rows from the server
			return sc, nil
		}
		if err != nil {
			return sc, err
		}
		if res.Cursor != nil {
			// print the cursor to stdout here.
			sc = p.serializeCursor(res.Cursor)
		}
		if len(res.Result) > 0 {
			for _, result := range res.Result {
				qr := sqltypes.Proto3ToResult(result)
				sqlResult := &sqltypes.Result{
					Fields: result.Fields,
				}
				sqlResult.Rows = append(sqlResult.Rows, qr.Rows[0])
				// print AirbyteRecord messages to stdout here.
				p.printQueryResult(os.Stdout, sqlResult, keyspaceOrDatabase, s.Name)
			}
		}
	}

	return sc, nil
}

func (p PlanetScaleEdgeDatabase) serializeCursor(cursor *psdbdatav1.TableCursor) *SerializedCursor {
	b, _ := json.Marshal(cursor)

	sc := &SerializedCursor{
		Cursor: string(b),
	}
	return sc
}

// printQueryResult will pretty-print an AirbyteRecordMessage to the logger.
// Copied from vtctl/query.go
func (p PlanetScaleEdgeDatabase) printQueryResult(writer io.Writer, qr *sqltypes.Result, tableNamespace, tableName string) {
	var data = make(map[string]interface{})

	columns := make([]string, 0, len(qr.Fields))
	for _, field := range qr.Fields {
		columns = append(columns, field.Name)
	}
	for _, row := range qr.Rows {
		for idx, val := range row {
			if idx < len(columns) {
				data[columns[idx]] = val
			}
		}
		p.Logger.Record(writer, tableNamespace, tableName, data)
	}
}