package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
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
	batch  []*batchedRecord
}

type batchedRecord struct {
	TableNamespace string
	TableName      string
	Data           map[string]interface{}
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

func (p PlanetScaleEdgeDatabase) ListShards(ctx context.Context, psc PlanetScaleConnection) ([]string, error) {
	return PlanetScaleMySQLDatabase{}.ListShards(ctx, psc)
}

func (p PlanetScaleEdgeDatabase) Read(ctx context.Context, w io.Writer, ps PlanetScaleConnection, s ConfiguredStream, maxReadDuration time.Duration, tc *psdbdatav1.TableCursor) (*SerializedCursor, error) {
	var (
		err     error
		sc      *SerializedCursor
		cancel  context.CancelFunc
		hasRows bool
	)

	table := s.Stream

	p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("will stop syncing after %v", maxReadDuration))
	//now := time.Now()
	//for time.Since(now) < maxReadDuration {

	p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("syncing rows for stream [%v] in namespace [%v] with cursor [%v]]", table.Name, table.Namespace, tc))
	if s.IncrementalSyncRequested() {
		p.Logger.Log(LOGLEVEL_INFO, "peeking to see if there's any new rows")
		peekCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		hasRows, _, _ = p.sync(peekCtx, tc, table, ps, true)
		if !hasRows {
			p.Logger.Log(LOGLEVEL_INFO, "no new rows found, exiting")
			return p.serializeCursor(tc), nil
		}
		p.Logger.Log(LOGLEVEL_INFO, "new rows found, continuing")
	}

	ctx, cancel = context.WithTimeout(ctx, maxReadDuration)
	defer cancel()
	_, tc, err = p.sync(ctx, tc, table, ps, false)
	if tc != nil {
		sc = p.serializeCursor(tc)
	}
	if err != nil {
		if s, ok := status.FromError(err); ok {
			// if the error is anything other than server timeout, keep going
			if s.Code() != codes.DeadlineExceeded {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("Got error [%v], Returning with cursor :[%v] after server timeout", s.Code(), tc))
				return sc, nil
			} else {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("Continuing with cursor :[%v] after server timeout", tc))
			}
		} else {
			p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("non-grpc error [%v]]", err))
			return sc, err
		}
		//}
	}

	p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("done syncing after %v", maxReadDuration))
	return sc, nil
}

func (p PlanetScaleEdgeDatabase) hasNewRows(ctx context.Context, tc *psdbdatav1.TableCursor, s Stream, ps PlanetScaleConnection) (bool, error) {
	tlsConfig := options.DefaultTLSConfig()
	var err error
	pool := psdbpool.New(
		router.NewSingleRoute(ps.Host),
		options.WithConnectionPool(4),
		options.WithTLSConfig(tlsConfig),
	)
	auth, err := authorization.NewBasicAuth(ps.Username, ps.Password)
	if err != nil {
		return false, err
	}

	conn, err := pool.GetWithAuth(ctx, auth)
	if err != nil {
		return false, err
	}
	defer conn.Release()

	return true, err
}

func (p PlanetScaleEdgeDatabase) sync(ctx context.Context, tc *psdbdatav1.TableCursor, s Stream, ps PlanetScaleConnection, peek bool) (bool, *psdbdatav1.TableCursor, error) {
	defer p.Logger.Flush()
	tlsConfig := options.DefaultTLSConfig()
	var err error
	pool := psdbpool.New(
		router.NewSingleRoute(ps.Host),
		options.WithConnectionPool(4),
		options.WithTLSConfig(tlsConfig),
	)
	auth, err := authorization.NewBasicAuth(ps.Username, ps.Password)
	if err != nil {
		return false, tc, err
	}

	conn, err := pool.GetWithAuth(ctx, auth)
	if err != nil {
		return false, tc, err
	}
	defer conn.Release()

	sReq := &psdbdatav1.SyncRequest{
		TableName: s.Name,
		Cursor:    tc,
	}

	c, err := conn.Sync(ctx, sReq)
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

	return false, tc, nil
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
func (p PlanetScaleEdgeDatabase) printQueryResult(qr *sqltypes.Result, tableNamespace, tableName string) {
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
		p.Logger.Record(tableNamespace, tableName, data)
	}
}
