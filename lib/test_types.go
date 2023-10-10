package lib

import (
	"context"
	"io"

	"github.com/pkg/errors"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"google.golang.org/grpc"
)

type dbLogMessage struct {
	message string
}
type dbLogger struct {
	messages []dbLogMessage
}

func (dbl *dbLogger) Info(s string) {
	dbl.messages = append(dbl.messages, dbLogMessage{
		message: s,
	})
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

type (
	BuildSchemaFunc     func(ctx context.Context, psc PlanetScaleSource, schemaBuilder SchemaBuilder) error
	PingContextFunc     func(context.Context, PlanetScaleSource) error
	GetVitessShardsFunc func(ctx context.Context, psc PlanetScaleSource) ([]string, error)
	TestMysqlClient     struct {
		BuildSchemaFn     BuildSchemaFunc
		PingContextFn     PingContextFunc
		GetVitessShardsFn GetVitessShardsFunc
	}
)

func (t TestMysqlClient) BuildSchema(ctx context.Context, psc PlanetScaleSource, schemaBuilder SchemaBuilder) error {
	if t.BuildSchemaFn != nil {
		return t.BuildSchemaFn(ctx, psc, schemaBuilder)
	}

	panic("BuildSchema is not implemented")
}

func (t TestMysqlClient) PingContext(ctx context.Context, source PlanetScaleSource) error {
	if t.PingContextFn != nil {
		return t.PingContextFn(ctx, source)
	}

	panic("PingContext is not implemented")
}

func (t TestMysqlClient) GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	if t.GetVitessShardsFn != nil {
		return t.GetVitessShardsFn(ctx, psc)
	}
	panic("GetvitessShards is not implemented")
}

func (t TestMysqlClient) Close() error {
	return nil
}

type (
	ReadFunc       func(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, columns []string, tc *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor, onUpdate OnUpdate) (*SerializedCursor, error)
	CanConnectFunc func(ctx context.Context, ps PlanetScaleSource) error
	ListShardsFunc func(ctx context.Context, ps PlanetScaleSource) ([]string, error)

	TestConnectClient struct {
		ReadFn       ReadFunc
		CanConnectFn CanConnectFunc
		ListShardsFn ListShardsFunc
	}
)

func (tcc *TestConnectClient) ListShards(ctx context.Context, ps PlanetScaleSource) ([]string, error) {
	if tcc.ListShardsFn != nil {
		return tcc.ListShardsFn(ctx, ps)
	}

	panic("implement me")
}

func (tcc *TestConnectClient) CanConnect(ctx context.Context, ps PlanetScaleSource) error {
	if tcc.CanConnectFn != nil {
		return tcc.CanConnectFn(ctx, ps)
	}
	return errors.New("CanConnect is Unimplemented")
}

func (tcc *TestConnectClient) Read(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, columns []string, lastKnownPosition *psdbconnect.TableCursor, onResult OnResult, onCursor OnCursor, onUpdate OnUpdate) (*SerializedCursor, error) {
	if tcc.ReadFn != nil {
		return tcc.ReadFn(ctx, logger, ps, tableName, columns, lastKnownPosition, onResult, onCursor, onUpdate)
	}

	return nil, errors.New("Read is Unimplemented")
}

func NewTestConnectClient(r ReadFunc) ConnectClient {
	return &TestConnectClient{ReadFn: r}
}
