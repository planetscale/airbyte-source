package internal

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"os"
	"testing"
	"time"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

func TestRead_CanPickPrimaryForShardedKeyspaces(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger:       NewLogger(b),
		Mysql:        tma,
		readDuration: 5 * time.Second,
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
			return syncClient, errors.New("all done")
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
	assert.ErrorContains(t, err, "all done")
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
	assert.False(t, tma.PingContextFnInvoked)
	assert.False(t, tma.GetVitessTabletsFnInvoked)
}

func TestDiscover_CanPickRightAirbyteType(t *testing.T) {
	var tests = []struct {
		MysqlType      string
		JSONSchemaType string
		AirbyteType    string
	}{
		{
			MysqlType:      "int(32)",
			JSONSchemaType: "integer",
			AirbyteType:    "",
		},
		{
			MysqlType:      "tinyint(1)",
			JSONSchemaType: "boolean",
			AirbyteType:    "",
		},
		{
			MysqlType:      "bigint(16)",
			JSONSchemaType: "string",
			AirbyteType:    "big_integer",
		},
		{
			MysqlType:      "bigint unsigned",
			JSONSchemaType: "string",
			AirbyteType:    "big_integer",
		},
		{
			MysqlType:      "bigint zerofill",
			JSONSchemaType: "string",
			AirbyteType:    "big_integer",
		},
		{
			MysqlType:      "datetime",
			JSONSchemaType: "string",
			AirbyteType:    "timestamp_with_timezone",
		},
		{
			MysqlType:      "date",
			JSONSchemaType: "string",
			AirbyteType:    "date",
		},
		{
			MysqlType:      "text",
			JSONSchemaType: "string",
			AirbyteType:    "",
		},
		{
			MysqlType:      "varchar(256)",
			JSONSchemaType: "string",
			AirbyteType:    "",
		},
	}

	for _, typeTest := range tests {

		t.Run(fmt.Sprintf("mysql_type_%v", typeTest.MysqlType), func(t *testing.T) {
			p := getJsonSchemaType(typeTest.MysqlType)
			assert.Equal(t, typeTest.AirbyteType, p.AirbyteType)
			assert.Equal(t, typeTest.JSONSchemaType, p.Type)
		})
	}
}
func TestRead_CanPickPrimaryForUnshardedKeyspaces(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger:       NewLogger(b),
		Mysql:        tma,
		readDuration: 5 * time.Second,
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
			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
			return syncClient, errors.New("all done")
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
	assert.ErrorContains(t, err, "all done")
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
	assert.False(t, tma.PingContextFnInvoked)
	assert.False(t, tma.GetVitessTabletsFnInvoked)
}

func TestRead_CanReturnOriginalCursorIfNoNewFound(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger:       NewLogger(b),
		Mysql:        tma,
		readDuration: 5 * time.Second,
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
			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
			return syncClient, errors.New("all done")
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
	assert.ErrorContains(t, err, "all done")
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestRead_CanReturnNewCursorIfNewFound(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger:       NewLogger(b),
		Mysql:        tma,
		readDuration: 5 * time.Second,
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
	esc, err := TableCursorToSerializedCursor(newTC)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestRead_CanContinueAfterDeadlineExceeded(t *testing.T) {
	tma := getTestMysqlAccess()
	tal := &testAirbyteLogger{}
	//b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger:       tal,
		Mysql:        tma,
		readDuration: 5 * time.Second,
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

	// by default connectSyncClientMock returns an io.EOF when there are no
	// more responses to send.
	syncClient := &connectSyncClientMock{
		sendDeadLineExceeded: true,
		syncResponses: []*psdbconnect.SyncResponse{
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
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}

	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	logLines := tal.logMessages[LOGLEVEL_INFO]
	// a DeadLineExceeded error means that the server timed out and that the client can retry.
	continueMessageFound := false
	for _, line := range logLines {
		continueMessageFound = continueMessageFound || line == "Continuing with cursor after server timeout"
	}
	assert.True(t, continueMessageFound, "Should continue after Server disconnects")
	// an io.EOF should mean that the server is done syncing all rows for this table.
	assert.Contains(t, logLines[len(logLines)-1], "Done synchronizing rows for stream [customers]")
	esc, err := TableCursorToSerializedCursor(newTC)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 2, cc.syncFnInvokedCount)
}

func TestRead_CanEndRecvAfterNonDeadlineExceeded(t *testing.T) {
	tma := getTestMysqlAccess()
	tal := &testAirbyteLogger{}
	//b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger:       tal,
		Mysql:        tma,
		readDuration: 5 * time.Second,
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

	// by default connectSyncClientMock returns an io.EOF when there are no
	// more responses to send.
	syncClient := &connectSyncClientMock{
		sendNonEOF: true,
		syncResponses: []*psdbconnect.SyncResponse{
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
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}

	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	logLines := tal.logMessages[LOGLEVEL_INFO]
	// an io.EOF should mean that the server is done syncing all rows for this table.
	assert.Contains(t, logLines[len(logLines)-1], "Got error [FailedPrecondition], Returning with cursor")
	esc, err := TableCursorToSerializedCursor(newTC)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestRead_CanEndRecvAfterEof(t *testing.T) {
	tma := getTestMysqlAccess()
	tal := &testAirbyteLogger{}
	//b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger:       tal,
		Mysql:        tma,
		readDuration: 5 * time.Second,
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

	// by default connectSyncClientMock returns an io.EOF when there are no
	// more responses to send.
	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
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
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}

	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	logLines := tal.logMessages[LOGLEVEL_INFO]
	// an io.EOF should mean that the server is done syncing all rows for this table.
	assert.Equal(t, "Done synchronizing rows for stream [customers]", logLines[len(logLines)-1])
	esc, err := TableCursorToSerializedCursor(newTC)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestRead_CanLogResults(t *testing.T) {
	tma := getTestMysqlAccess()
	tal := testAirbyteLogger{}
	ped := PlanetScaleEdgeDatabase{
		Logger:       &tal,
		Mysql:        tma,
		readDuration: 5 * time.Second,
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

	result := []*query.QueryResult{
		sqltypes.ResultToProto3(sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"pid|description",
			"int64|varbinary"),
			"1|keyboard",
			"2|monitor",
		)),
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{Cursor: newTC, Result: result},
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
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "products",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	assert.NotNil(t, sc)
	assert.Equal(t, 2, len(tal.records["connect-test.products"]))
	records := tal.records["connect-test.products"]
	keyboardFound := false
	monitorFound := false
	for _, r := range records {
		id, err := r["pid"].(sqltypes.Value).ToInt64()
		assert.NoError(t, err)
		if id == 1 {
			assert.False(t, keyboardFound, "should not find keyboard twice")
			keyboardFound = true
			assert.Equal(t, "keyboard", r["description"].(sqltypes.Value).ToString())
		}

		if id == 2 {
			assert.False(t, monitorFound, "should not find monitor twice")
			monitorFound = true
			assert.Equal(t, "monitor", r["description"].(sqltypes.Value).ToString())
		}
	}
	assert.True(t, keyboardFound)
	assert.True(t, monitorFound)
}

func getTestMysqlAccess() *mysqlAccessMock {
	tma := mysqlAccessMock{
		PingContextFn: func(ctx context.Context, source PlanetScaleSource) error {
			return nil
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
