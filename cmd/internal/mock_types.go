package internal

import (
	"context"
	"database/sql"
	"io"

	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"
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

func (testAirbyteLogger) Catalog(catalog Catalog) {
	//TODO implement me
	panic("implement me")
}

func (testAirbyteLogger) ConnectionStatus(status ConnectionStatus) {
	//TODO implement me
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

func (testAirbyteLogger) State(syncState SyncState) {
	//TODO implement me
	panic("implement me")
}

func (testAirbyteLogger) Error(error string) {
	//TODO implement me
	panic("implement me")
}

type vstreamClientMock struct {
	vstreamFn             func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error)
	vstreamFnInvoked      bool
	vstreamFnInvokedCount int
}

type vtgateVStreamClientMock struct {
	lastResponseSent int
	vstreamResponses []*vtgate.VStreamResponse
	grpc.ClientStream
}

func (x *vtgateVStreamClientMock) Recv() (*vtgate.VStreamResponse, error) {
	if x.lastResponseSent >= len(x.vstreamResponses) {
		return nil, io.EOF
	}
	x.lastResponseSent += 1
	return x.vstreamResponses[x.lastResponseSent-1], nil
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
	//TODO implement me
	panic("implement me")
}

func (mysqlAccessMock) GetTableSchema(ctx context.Context, source PlanetScaleSource, s string) (map[string]PropertyType, error) {
	//TODO implement me
	panic("implement me")
}

func (mysqlAccessMock) GetTablePrimaryKeys(ctx context.Context, source PlanetScaleSource, s string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (mysqlAccessMock) QueryContext(ctx context.Context, psc PlanetScaleSource, query string, args ...interface{}) (*sql.Rows, error) {
	//TODO implement me
	panic("implement me")
}

func (tma *mysqlAccessMock) GetVitessTablets(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error) {
	tma.GetVitessTabletsFnInvoked = true
	return tma.GetVitessTabletsFn(ctx, psc)
}

func (mysqlAccessMock) GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	//TODO implement me
	panic("implement me")
}
func (mysqlAccessMock) Close() error { return nil }
