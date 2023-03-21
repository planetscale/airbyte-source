package airbyte_source

import (
	"context"
	"github.com/planetscale/airbyte-source/cmd/types"
	"github.com/planetscale/airbyte-source/shared"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
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

type discoverSchemaResponse struct {
	catalog types.Catalog
	err     error
}

type testDatabase struct {
	connectResponse        canConnectResponse
	discoverSchemaResponse discoverSchemaResponse
}

func (td testDatabase) CanConnect(ctx context.Context, ps types.PlanetScaleSource) error {
	return td.connectResponse.err
}

func (td testDatabase) HasTabletType(ctx context.Context, psc types.PlanetScaleSource, tt psdbconnect.TabletType) (bool, error) {
	return true, nil
}

func (td testDatabase) DiscoverSchema(ctx context.Context, ps types.PlanetScaleSource) (types.Catalog, error) {
	return td.discoverSchemaResponse.catalog, td.discoverSchemaResponse.err
}

func (td testDatabase) Read(ctx context.Context, ps types.PlanetScaleAuthentication, keyspaceName string, tableName string, lastKnownPosition *psdbconnect.TableCursor, onResult shared.OnResult, onCursor shared.OnCursor) (*types.SerializedCursor, error) {
	// TODO implement me
	panic("implement me")
}

func (td testDatabase) Close() error {
	return nil
}

func (td testDatabase) ListShards(ctx context.Context, ps types.PlanetScaleSource) ([]string, error) {
	panic("implement me")
}
