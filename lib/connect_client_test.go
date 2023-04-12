package lib

import (
	"context"
	"fmt"
	"testing"

	"vitess.io/vitess/go/vt/proto/query"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/sqltypes"
)

func TestRead_CanPeekBeforeRead(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{
				Cursor: tc,
			},
			{
				Cursor: tc,
			},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, "current", in.Cursor.Position)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{}
	onRow := func(*sqltypes.Result) error {
		return nil
	}
	onCursor := func(*psdbconnect.TableCursor) error {
		return nil
	}
	sc, err := ped.Read(context.Background(), dbl, ps, "customers", tc, onRow, onCursor)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestRead_CanEarlyExitIfNoNewVGtidInPeek(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{Cursor: tc},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, "current", in.Cursor.Position)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{}
	onRow := func(*sqltypes.Result) error {
		return nil
	}
	onCursor := func(*psdbconnect.TableCursor) error {
		return nil
	}
	sc, err := ped.Read(context.Background(), dbl, ps, "customers", tc, onRow, onCursor)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc, "should return original cursor if no new rows found")
	assert.Equal(t, 1, cc.syncFnInvokedCount)
	assert.Contains(t, dbl.messages[len(dbl.messages)-1].message, "no new rows found, exiting")
}

func TestRead_CanPickPrimaryForShardedKeyspaces(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	tc := &psdbconnect.TableCursor{
		Shard:    "40-80",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{Cursor: tc},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	onRow := func(*sqltypes.Result) error {
		return nil
	}
	onCursor := func(*psdbconnect.TableCursor) error {
		return nil
	}
	sc, err := ped.Read(context.Background(), dbl, ps, "customers", tc, onRow, onCursor)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestRead_CanReturnNewCursorIfNewFound(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}
	newTC := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "I_AM_FARTHER_IN_THE_BINLOG",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{Cursor: newTC},
			{Cursor: newTC},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	onRow := func(*sqltypes.Result) error {
		return nil
	}
	onCursor := func(*psdbconnect.TableCursor) error {
		return nil
	}
	sc, err := ped.Read(context.Background(), dbl, ps, "customers", tc, onRow, onCursor)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(newTC)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 2, cc.syncFnInvokedCount)
}

func TestRead_CanStopAtWellKnownCursor(t *testing.T) {
	dbl := &dbLogger{}
	ped := connectClient{}

	numResponses := 10
	// when the client tries to get the "current" vgtid,
	// we return the ante-penultimate element of the array.
	currentVGtidPosition := (numResponses * 3) - 4
	// this is the next vgtid that should stop the sync session.
	nextVGtidPosition := currentVGtidPosition + 1
	responses := make([]*psdbconnect.SyncResponse, 0, numResponses)
	for i := 0; i < numResponses; i++ {
		// this simulates multiple events being returned, for the same vgtid, from vstream
		for x := 0; x < 3; x++ {
			var result []*query.QueryResult
			if x == 2 {
				result = []*query.QueryResult{
					sqltypes.ResultToProto3(sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"pid|description",
						"int64|varbinary"),
						fmt.Sprintf("%v|keyboard", i+1),
						fmt.Sprintf("%v|monitor", i+2),
					)),
				}
			}

			vgtid := fmt.Sprintf("e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", i)
			responses = append(responses, &psdbconnect.SyncResponse{
				Cursor: &psdbconnect.TableCursor{
					Shard:    "-",
					Keyspace: "connect-test",
					Position: vgtid,
				},
				Result: result,
			})
		}
	}

	syncClient := &connectSyncClientMock{
		syncResponses: responses,
	}

	getCurrentVGtidClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			responses[currentVGtidPosition],
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
			if in.Cursor.Position == "current" {
				return getCurrentVGtidClient, nil
			}

			return syncClient, nil
		},
	}

	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	rowCounter := 0
	onRow := func(*sqltypes.Result) error {
		rowCounter += 1
		return nil
	}
	onCursor := func(*psdbconnect.TableCursor) error {
		return nil
	}
	sc, err := ped.Read(context.Background(), dbl, ps, "customers", responses[0].Cursor, onRow, onCursor)

	assert.NoError(t, err)
	// sync should start at the first vgtid
	esc, err := TableCursorToSerializedCursor(responses[nextVGtidPosition].Cursor)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 2, cc.syncFnInvokedCount)

	assert.Equal(t, "[connect-test:customers shard : -] Finished reading all rows for table [customers]", dbl.messages[len(dbl.messages)-1].message)
	assert.Equal(t, 2*(nextVGtidPosition/3), rowCounter)
}
