package internal

import (
	"context"
	"database/sql"
	"io"

	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"
)

type testAirbyteLogEntry struct {
	level   string
	message string
}

var _ AirbyteLogger = (*testAirbyteLogger)(nil)

type testAirbyteLogger struct {
	logMessages        []testAirbyteLogEntry
	logMessagesByLevel map[string][]string
	records            map[string][]map[string]interface{}
}

// QueueFull implements AirbyteLogger.
func (tal *testAirbyteLogger) QueueFull() bool {
	return len(tal.records) > 0
}

func (tal *testAirbyteLogger) Log(level, message string) {
	if tal.logMessagesByLevel == nil {
		tal.logMessagesByLevel = map[string][]string{}
	}
	tal.logMessagesByLevel[level] = append(tal.logMessagesByLevel[level], message)
	tal.logMessages = append(tal.logMessages, testAirbyteLogEntry{level, message})
}

func (testAirbyteLogger) Catalog(catalog Catalog) {
	// TODO implement me
	panic("implement me")
}

func (testAirbyteLogger) ConnectionStatus(status ConnectionStatus) {
	// TODO implement me
	panic("implement me")
}

func (tal *testAirbyteLogger) Record(tableNamespace, tableName string, data map[string]interface{}) error {
	if tal.records == nil {
		tal.records = map[string][]map[string]interface{}{}
	}
	key := tableNamespace + "." + tableName
	tal.records[key] = append(tal.records[key], data)
	return nil
}

func (testAirbyteLogger) Flush() error {
	return nil
}

func (testAirbyteLogger) State(syncState SyncState) {
	// TODO implement me
	panic("implement me")
}

func (testAirbyteLogger) Error(error string) {
	// TODO implement me
	panic("implement me")
}

type vstreamClientMock struct {
	vstreamFn             func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error)
	vstreamFnInvoked      bool
	vstreamFnInvokedCount int
}

type vstreamResponse struct {
	response *vtgate.VStreamResponse
	err      error
}

type vtgateVStreamClientMock struct {
	lastResponseSent int
	vstreamResponses []*vstreamResponse
	grpc.ClientStream
}

func (x *vtgateVStreamClientMock) Recv() (*vtgate.VStreamResponse, error) {
	if x.lastResponseSent >= len(x.vstreamResponses) {
		return nil, io.EOF
	}
	x.lastResponseSent += 1
	return x.vstreamResponses[x.lastResponseSent-1].response, x.vstreamResponses[x.lastResponseSent-1].err
}

func (x *vstreamClientMock) CloseSession(context.Context, *vtgate.CloseSessionRequest, ...grpc.CallOption) (*vtgate.CloseSessionResponse, error) {
	return nil, nil
}

func (x *vstreamClientMock) Execute(context.Context, *vtgate.ExecuteRequest, ...grpc.CallOption) (*vtgate.ExecuteResponse, error) {
	return nil, nil
}

func (x *vstreamClientMock) ExecuteBatch(context.Context, *vtgate.ExecuteBatchRequest, ...grpc.CallOption) (*vtgate.ExecuteBatchResponse, error) {
	return nil, nil
}

func (x *vstreamClientMock) Prepare(context.Context, *vtgate.PrepareRequest, ...grpc.CallOption) (*vtgate.PrepareResponse, error) {
	return nil, nil
}

func (x *vstreamClientMock) ResolveTransaction(context.Context, *vtgate.ResolveTransactionRequest, ...grpc.CallOption) (*vtgate.ResolveTransactionResponse, error) {
	return nil, nil
}

func (x *vstreamClientMock) StreamExecute(context.Context, *vtgate.StreamExecuteRequest, ...grpc.CallOption) (vtgateservice.Vitess_StreamExecuteClient, error) {
	return nil, nil
}

func (c *vstreamClientMock) VStream(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
	c.vstreamFnInvoked = true
	c.vstreamFnInvokedCount += 1
	return c.vstreamFn(ctx, in, opts...)
}

type mysqlAccessMock struct {
	PingContextFn             func(ctx context.Context, source PlanetScaleSource) error
	PingContextFnInvoked      bool
	GetVitessTabletsFn        func(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error)
	GetVitessTabletsFnInvoked bool
}

func (tma *mysqlAccessMock) PingContext(ctx context.Context, source PlanetScaleSource) error {
	tma.PingContextFnInvoked = true
	return tma.PingContextFn(ctx, source)
}

func (mysqlAccessMock) GetTableNames(ctx context.Context, source PlanetScaleSource) ([]string, error) {
	// TODO implement me
	panic("implement me")
}

func (mysqlAccessMock) GetTableSchema(ctx context.Context, source PlanetScaleSource, s string) (map[string]PropertyType, error) {
	// TODO implement me
	panic("implement me")
}

func (mysqlAccessMock) GetTablePrimaryKeys(ctx context.Context, source PlanetScaleSource, s string) ([]string, error) {
	// TODO implement me
	panic("implement me")
}

func (mysqlAccessMock) QueryContext(ctx context.Context, psc PlanetScaleSource, query string, args ...interface{}) (*sql.Rows, error) {
	// TODO implement me
	panic("implement me")
}

func (tma *mysqlAccessMock) GetVitessTablets(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error) {
	tma.GetVitessTabletsFnInvoked = true
	return tma.GetVitessTabletsFn(ctx, psc)
}

func (mysqlAccessMock) GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	// TODO implement me
	panic("implement me")
}
func (mysqlAccessMock) Close() error { return nil }
