package shared

import (
	"context"
	"fmt"
	"github.com/planetscale/airbyte-source/cmd/types"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	grpcclient "github.com/planetscale/psdb/core/pool"
	clientoptions "github.com/planetscale/psdb/core/pool/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"vitess.io/vitess/go/sqltypes"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

type (
	OnResult func(string, string, *sqltypes.Result) error
	OnCursor func(*psdbconnect.TableCursor) error
)

// PlanetScaleDatabase is a general purpose interface
// that defines all the data access methods needed for the PlanetScale Airbyte source to function.
type PlanetScaleDatabase interface {
	CanConnect(ctx context.Context, ps types.PlanetScaleSource) error
	ListShards(ctx context.Context, ps types.PlanetScaleSource) ([]string, error)
	Read(ctx context.Context, ps types.PlanetScaleAuthentication, keyspaceName string, tableName string, lastKnownPosition *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor) (*types.SerializedCursor, error)
	Close() error
}

func NewPlanetScaleEdgeDatabase(mysql PlanetScaleEdgeMysqlAccess, logger types.Logger) PlanetScaleDatabase {
	return &PlanetScaleEdgeDatabase{
		Mysql:  mysql,
		Logger: logger,
	}
}

// PlanetScaleEdgeDatabase is an implementation of the PlanetScaleDatabase interface defined above.
// It uses the mysql interface provided by PlanetScale for all schema/shard/tablet discovery and
// the grpc API for incrementally syncing rows from PlanetScale.
type PlanetScaleEdgeDatabase struct {
	Logger   types.Logger
	Mysql    PlanetScaleEdgeMysqlAccess
	clientFn func(ctx context.Context, ps types.PlanetScaleAuthentication) (psdbconnect.ConnectClient, error)
}

func (p PlanetScaleEdgeDatabase) CanConnect(ctx context.Context, psc types.PlanetScaleSource) error {
	if err := p.checkEdgePassword(ctx, psc); err != nil {
		return errors.Wrap(err, "Unable to initialize Connect Session")
	}

	return p.Mysql.PingContext(ctx, psc)
}

func (p PlanetScaleEdgeDatabase) checkEdgePassword(ctx context.Context, psc types.PlanetScaleSource) error {
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

func (p PlanetScaleEdgeDatabase) Close() error {
	return p.Mysql.Close()
}

func (p PlanetScaleEdgeDatabase) ListShards(ctx context.Context, psc types.PlanetScaleSource) ([]string, error) {
	return p.Mysql.GetVitessShards(ctx, psc)
}

// Read streams rows from a table given a starting cursor.
// 1. We will get the latest vgtid for a given table in a shard when a sync session starts.
// 2. This latest vgtid is now the stopping point for this sync session.
// 3. Ask vstream to stream from the last known vgtid
// 4. When we reach the stopping point, read all rows available at this vgtid
// 5. End the stream when (a) a vgtid newer than latest vgtid is encountered or (b) the timeout kicks in.
func (p PlanetScaleEdgeDatabase) Read(ctx context.Context, ps types.PlanetScaleAuthentication, keyspaceName string, tableName string, lastKnownPosition *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor) (*types.SerializedCursor, error) {
	var (
		err                     error
		sErr                    error
		currentSerializedCursor *types.SerializedCursor
	)

	tabletType := psdbconnect.TabletType_primary
	currentPosition := lastKnownPosition
	readDuration := 1 * time.Minute
	preamble := fmt.Sprintf("[%v:%v shard : %v] ", keyspaceName, tableName, currentPosition.Shard)
	for {
		p.Logger.Log(types.LOGLEVEL_INFO, preamble+"peeking to see if there's any new rows")
		latestCursorPosition, lcErr := p.getLatestCursorPosition(ctx, currentPosition.Shard, currentPosition.Keyspace, tableName, ps, tabletType)
		if lcErr != nil {
			return currentSerializedCursor, errors.Wrap(err, "Unable to get latest cursor position")
		}

		// the current vgtid is the same as the last synced vgtid, no new rows.
		if latestCursorPosition == currentPosition.Position {
			p.Logger.Log(types.LOGLEVEL_INFO, preamble+"no new rows found, exiting")
			return types.TableCursorToSerializedCursor(currentPosition)
		}
		p.Logger.Log(types.LOGLEVEL_INFO, fmt.Sprintf("new rows found, syncing rows for %v", readDuration))
		p.Logger.Log(types.LOGLEVEL_INFO, fmt.Sprintf(preamble+"syncing rows with cursor [%v]", currentPosition))

		currentPosition, err = p.sync(ctx, currentPosition, latestCursorPosition, keyspaceName, tableName, ps, tabletType, readDuration, onResult, onCursor)
		if currentPosition.Position != "" {
			currentSerializedCursor, sErr = types.TableCursorToSerializedCursor(currentPosition)
			if sErr != nil {
				// if we failed to serialize here, we should bail.
				return currentSerializedCursor, errors.Wrap(sErr, "unable to serialize current position")
			}
		}
		if err != nil {
			if s, ok := status.FromError(err); ok {
				// if the error is anything other than server timeout, keep going
				if s.Code() != codes.DeadlineExceeded {
					p.Logger.Log(types.LOGLEVEL_INFO, fmt.Sprintf("%v Got error [%v], Returning with cursor :[%v] after server timeout", preamble, s.Code(), currentPosition))
					return currentSerializedCursor, nil
				} else {
					p.Logger.Log(types.LOGLEVEL_INFO, preamble+"Continuing with cursor after server timeout")
				}
			} else if errors.Is(err, io.EOF) {
				p.Logger.Log(types.LOGLEVEL_INFO, fmt.Sprintf("%vFinished reading all rows for table [%v]", preamble, tableName))
				return currentSerializedCursor, nil
			} else {
				p.Logger.Log(types.LOGLEVEL_INFO, fmt.Sprintf("non-grpc error [%v]]", err))
				return currentSerializedCursor, err
			}
		}
	}
}

func (p PlanetScaleEdgeDatabase) sync(ctx context.Context, tc *psdbconnect.TableCursor, stopPosition, keyspaceName, tableName string, ps types.PlanetScaleAuthentication, tabletType psdbconnect.TabletType, readDuration time.Duration, onResult OnResult, onCursor OnCursor) (*psdbconnect.TableCursor, error) {
	defer p.Logger.Flush()
	ctx, cancel := context.WithTimeout(ctx, readDuration)
	defer cancel()

	var (
		err    error
		client psdbconnect.ConnectClient
	)

	if p.clientFn == nil {
		conn, err := grpcclient.Dial(ctx, ps.GetHostname(),
			clientoptions.WithDefaultTLSConfig(),
			clientoptions.WithCompression(true),
			clientoptions.WithConnectionPool(1),
			clientoptions.WithExtraCallOption(
				ps.GetAuthorizationHeader().CallOption(),
			),
		)
		if err != nil {
			return tc, err
		}
		defer conn.Close()
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

	p.Logger.Log(types.LOGLEVEL_INFO, fmt.Sprintf("Syncing with cursor position : [%v], using last known PK : %v, stop cursor is : [%v]", tc.Position, tc.LastKnownPk != nil, stopPosition))

	sReq := &psdbconnect.SyncRequest{
		TableName:  tableName,
		Cursor:     tc,
		TabletType: tabletType,
	}

	c, err := client.Sync(ctx, sReq)
	if err != nil {
		return tc, err
	}

	// stop when we've reached the well known stop position for this sync session.
	watchForVgGtidChange := false

	for {

		res, err := c.Recv()
		if err != nil {
			return tc, err
		}

		if res.Cursor != nil {
			tc = res.Cursor

			if onCursor != nil {
				if err := onCursor(tc); err != nil {
					return tc, errors.Wrap(err, "unable to print cursor to stdout")
				}
			}
		}

		// Because of the ordering of events in a vstream
		// we receive the vgtid event first and then the rows.
		// the vgtid event might repeat, but they're ordered.
		// so we once we reach the desired stop vgtid, we stop the sync session
		// if we get a newer vgtid.
		watchForVgGtidChange = watchForVgGtidChange || tc.Position == stopPosition

		if len(res.Result) > 0 {
			for _, result := range res.Result {
				qr := sqltypes.Proto3ToResult(result)
				for _, row := range qr.Rows {
					sqlResult := &sqltypes.Result{
						Fields: result.Fields,
					}
					sqlResult.Rows = append(sqlResult.Rows, row)
					// print AirbyteRecord messages to stdout here.
					if onResult != nil {
						if err := onResult(keyspaceName, tableName, sqlResult); err != nil {
							return tc, errors.Wrap(err, "unable to print result to stdout")
						}
					}
				}
			}
		}

		if watchForVgGtidChange && tc.Position != stopPosition {
			return tc, io.EOF
		}
	}
}

func (p PlanetScaleEdgeDatabase) getLatestCursorPosition(ctx context.Context, shard, keyspace string, tableName string, ps types.PlanetScaleAuthentication, tabletType psdbconnect.TabletType) (string, error) {
	defer p.Logger.Flush()
	timeout := 45 * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var (
		err    error
		client psdbconnect.ConnectClient
	)

	if p.clientFn == nil {
		conn, err := grpcclient.Dial(ctx, ps.GetHostname(),
			clientoptions.WithDefaultTLSConfig(),
			clientoptions.WithCompression(true),
			clientoptions.WithConnectionPool(1),
			clientoptions.WithExtraCallOption(
				ps.GetAuthorizationHeader().CallOption(),
			),
		)
		if err != nil {
			return "", err
		}
		defer conn.Close()
		client = psdbconnect.NewConnectClient(conn)
	} else {
		client, err = p.clientFn(ctx, ps)
		if err != nil {
			return "", err
		}
	}

	sReq := &psdbconnect.SyncRequest{
		TableName: tableName,
		Cursor: &psdbconnect.TableCursor{
			Shard:    shard,
			Keyspace: keyspace,
			Position: "current",
		},
		TabletType: tabletType,
	}

	c, err := client.Sync(ctx, sReq)
	if err != nil {
		return "", nil
	}

	for {
		res, err := c.Recv()
		if err != nil {
			return "", err
		}

		if res.Cursor != nil {
			return res.Cursor.Position, nil
		}
	}
}
