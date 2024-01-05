package airbyte_source

import (
	"context"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/connectsdk/lib"
)

type testFileReader struct {
	content []byte
	err     error
}

func (tfr testFileReader) ReadFile(path string) ([]byte, error) {
	return tfr.content, tfr.err
}

type canConnectResponse struct {
	err error
}

type testConnectClient struct {
	connectResponse canConnectResponse
}

func (td testConnectClient) CanConnect(ctx context.Context, ps lib.PlanetScaleSource) error {
	return td.connectResponse.err
}

func (td testConnectClient) Read(context.Context, lib.DatabaseLogger, lib.PlanetScaleSource, string, []string, *psdbconnect.TableCursor, lib.OnResult, lib.OnCursor, lib.OnUpdate) (*lib.SerializedCursor, error) {
	// TODO implement me
	panic("implement me")
}

func (td testConnectClient) Close() error {
	return nil
}

func (td testConnectClient) ListShards(ctx context.Context, ps lib.PlanetScaleSource) ([]string, error) {
	panic("implement me")
}

type buildSchemaResponse struct {
	err error
}

type testMysqlClient struct {
	buildSchemaResponse buildSchemaResponse
}

func (tmc testMysqlClient) BuildSchema(ctx context.Context, psc lib.PlanetScaleSource, schemaBuilder lib.SchemaBuilder) error {
	return tmc.buildSchemaResponse.err
}

func (tmc testMysqlClient) PingContext(ctx context.Context, source lib.PlanetScaleSource) error {
	//TODO implement me
	panic("implement me")
}

func (tmc testMysqlClient) GetVitessShards(ctx context.Context, psc lib.PlanetScaleSource) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (tmc testMysqlClient) Close() error {
	//TODO implement me
	panic("implement me")
}
