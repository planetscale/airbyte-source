package shared

import (
	"context"
	"database/sql"
	"io"

	"github.com/planetscale/airbyte-source/cmd/types"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"google.golang.org/grpc"
)

type testAirbyteLogger struct {
	logMessages map[string][]string
	records     map[string][]map[string]interface{}
}

func (tal *testAirbyteLogger) Log(level, message string) {
	if tal.logMessages == nil {
		tal.logMessages = map[string][]string{}
	}
	tal.logMessages[level] = append(tal.logMessages[level], message)
}

func (testAirbyteLogger) Catalog(catalog types.Catalog) {
	// TODO implement me
	panic("implement me")
}

func (testAirbyteLogger) ConnectionStatus(status types.ConnectionStatus) {
	// TODO implement me
	panic("implement me")
}

func (tal *testAirbyteLogger) Record(tableNamespace, tableName string, data map[string]interface{}) {
	if tal.records == nil {
		tal.records = map[string][]map[string]interface{}{}
	}
	key := tableNamespace + "." + tableName
	tal.records[key] = append(tal.records[key], data)
}

func (testAirbyteLogger) Flush() {
}

func (testAirbyteLogger) State(syncState types.SyncState) {
	// TODO implement me
	panic("implement me")
}

func (testAirbyteLogger) Error(error string) {
	// TODO implement me
	panic("implement me")
}

type clientConnectionMock struct {
	syncFn             func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error)
	syncFnInvoked      bool
	syncFnInvokedCount int
}

type connectSyncClientMock struct {
	lastResponseSent int
	syncResponses    []*psdbconnect.SyncResponse
	grpc.ClientStream
}

func (x *connectSyncClientMock) Recv() (*psdbconnect.SyncResponse, error) {
	if x.lastResponseSent >= len(x.syncResponses) {
		return nil, io.EOF
	}
	x.lastResponseSent += 1
	return x.syncResponses[x.lastResponseSent-1], nil
}

func (c *clientConnectionMock) Sync(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
	c.syncFnInvoked = true
	c.syncFnInvokedCount += 1
	return c.syncFn(ctx, in, opts...)
}

type mysqlAccessMock struct {
	PingContextFn             func(ctx context.Context, source types.PlanetScaleSource) error
	PingContextFnInvoked      bool
	GetVitessTabletsFn        func(ctx context.Context, psc types.PlanetScaleSource) ([]types.VitessTablet, error)
	GetVitessTabletsFnInvoked bool
}

func (tma *mysqlAccessMock) PingContext(ctx context.Context, source types.PlanetScaleSource) error {
	tma.PingContextFnInvoked = true
	return tma.PingContextFn(ctx, source)
}

func (mysqlAccessMock) GetTableNames(ctx context.Context, source types.PlanetScaleSource) ([]string, error) {
	// TODO implement me
	panic("implement me")
}

func (mysqlAccessMock) GetTableSchema(ctx context.Context, source types.PlanetScaleSource, s string) (map[string]types.PropertyType, error) {
	// TODO implement me
	panic("implement me")
}

func (mysqlAccessMock) GetTablePrimaryKeys(ctx context.Context, source types.PlanetScaleSource, s string) ([]string, error) {
	// TODO implement me
	panic("implement me")
}

func (mysqlAccessMock) QueryContext(ctx context.Context, psc types.PlanetScaleSource, query string, args ...interface{}) (*sql.Rows, error) {
	// TODO implement me
	panic("implement me")
}

func (tma *mysqlAccessMock) GetVitessTablets(ctx context.Context, psc types.PlanetScaleSource) ([]types.VitessTablet, error) {
	tma.GetVitessTabletsFnInvoked = true
	return tma.GetVitessTabletsFn(ctx, psc)
}

func (mysqlAccessMock) GetVitessShards(ctx context.Context, psc types.PlanetScaleSource) ([]string, error) {
	// TODO implement me
	panic("implement me")
}
func (mysqlAccessMock) Close() error { return nil }
