package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

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

type PlanetScaleVstreamDatabase struct {
	grpcAddr string
	Logger   AirbyteLogger
}

type TableCursorState struct {
	SerializedCursor string `json:"cursor"`
}

func (p PlanetScaleVstreamDatabase) CanConnect(ctx context.Context, psc PlanetScaleConnection) (bool, error) {
	return PlanetScaleMySQLDatabase{}.CanConnect(ctx, psc)
}

func (p PlanetScaleVstreamDatabase) DiscoverSchema(ctx context.Context, psc PlanetScaleConnection) (Catalog, error) {
	return PlanetScaleMySQLDatabase{}.DiscoverSchema(ctx, psc)
}

func (p PlanetScaleVstreamDatabase) Read(ctx context.Context, w io.Writer, ps PlanetScaleConnection, s Stream, state string) error {
	return p.sync(ctx, state, s, ps)
}

func (p PlanetScaleVstreamDatabase) sync(ctx context.Context, state string, s Stream, ps PlanetScaleConnection) error {
	tlsConfig := options.DefaultTLSConfig()
	pool := psdbpool.New(
		router.NewSingleRoute(p.grpcAddr),
		options.WithConnectionPool(4),
		options.WithTLSConfig(tlsConfig),
	)
	auth, err := authorization.NewBasicAuth(ps.Username, ps.Password)
	if err != nil {
		return err
	}

	conn, err := pool.GetWithAuth(ctx, auth)
	if err != nil {
		return err
	}
	defer conn.Release()

	var (
		tc *psdbdatav1.TableCursor
	)

	if state == "" {
		tc = &psdbdatav1.TableCursor{
			Shard:    "-",
			Keyspace: s.Namespace,
			Position: "",
		}
	} else {
		tc, err = parseSyncState(state)
		if err != nil {
			fmt.Printf("\nUnable to read state, failed with error : [%v]\n", err)
			return err
		}
		fmt.Printf("Found existing state, continuing where we left off, state is [%v]", tc)
	}

	sReq := &psdbdatav1.SyncRequest{
		TableName: s.Name,
		Cursor:    tc,
	}

	c, err := conn.Sync(ctx, sReq)
	if err != nil {
		return nil
	}

	for {
		res, err := c.Recv()
		if errors.Is(err, io.EOF) {
			// we're done receiving rows from the server
			return nil
		}
		if res.Cursor != nil {
			// print the cursor to stdout here.
			p.printSyncState(os.Stdout, res.Cursor)
		}
		if len(res.Result) > 0 {
			for _, result := range res.Result {
				qr := sqltypes.Proto3ToResult(result)
				sqlResult := &sqltypes.Result{
					Fields: result.Fields,
				}
				sqlResult.Rows = append(sqlResult.Rows, qr.Rows[0])
				// print AirbyteRecord messages to stdout here.
				p.printQueryResult(os.Stdout, sqlResult, s.Namespace, s.Name)
			}
		}
	}
}

func parseSyncState(state string) (*psdbdatav1.TableCursor, error) {
	var tcs TableCursorState
	err := json.Unmarshal([]byte(state), &tcs)
	if err != nil {
		return nil, err
	}
	var tc psdbdatav1.TableCursor
	err = json.Unmarshal([]byte(tcs.SerializedCursor), &tc)
	return &tc, err
}

func (p PlanetScaleVstreamDatabase) printSyncState(writer io.Writer, cursor *psdbdatav1.TableCursor) {
	b, _ := json.Marshal(cursor)
	data := map[string]string{"cursor": string(b)}
	p.Logger.State(writer, data)
}

// printQueryResult will pretty-print an AirbyteRecordMessage to the logger.
// Copied from vtctl/query.go
func (p PlanetScaleVstreamDatabase) printQueryResult(writer io.Writer, qr *sqltypes.Result, tableNamespace, tableName string) {
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
