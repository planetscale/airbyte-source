package internal

import (
	"bytes"
	"context"
	psdbconnect "github.com/planetscale/connect/source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"os"
	"testing"
)

func TestCanPeekBeforeRead(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger(b),
		Mysql:  tma,
	}
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
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 3, cc.syncFnInvokedCount)
}

func TestCanEarlyExitIfNoRecordsInPeek(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger(b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestCanPickPrimaryForShardedKeyspaces(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger(b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "40-80",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{},
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
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestCanPickReplicaForUnshardedKeyspaces(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger(b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, psdbconnect.TabletType_replica, in.TabletType)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestCanReturnOriginalCursorIfNoNewFound(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger(b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, psdbconnect.TabletType_replica, in.TabletType)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestCanReturnNewCursorIfNewFound(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger(b),
		Mysql:  tma,
	}
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
			assert.Equal(t, psdbconnect.TabletType_replica, in.TabletType)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(newTC)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 3, cc.syncFnInvokedCount)
}
func getTestMysqlAccess() *mysqlAccessMock {
	tma := mysqlAccessMock{
		PingContextFn: func(ctx context.Context, source PlanetScaleSource) (bool, error) {
			return true, nil
		},
		GetVitessTabletsFn: func(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error) {
			return []VitessTablet{
				{
					Keyspace:   "connect-test",
					TabletType: TabletTypeToString(psdbconnect.TabletType_primary),
					State:      "SERVING",
				},
				{
					Keyspace:   "connect-test",
					TabletType: TabletTypeToString(psdbconnect.TabletType_replica),
					State:      "SERVING",
				},
			}, nil
		},
	}
	return &tma
}
