package airbyte_source

import (
	"context"
	"io"

	"github.com/planetscale/airbyte-source/cmd/internal"
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
	catalog internal.Catalog
	err     error
}

type testDatabase struct {
	connectResponse        canConnectResponse
	discoverSchemaResponse discoverSchemaResponse
}

func (td testDatabase) CanConnect(ctx context.Context, ps internal.PlanetScaleSource) error {
	return td.connectResponse.err
}

func (td testDatabase) HasTabletType(ctx context.Context, psc internal.PlanetScaleSource, tt psdbconnect.TabletType) (bool, error) {
	return true, nil
}

func (td testDatabase) DiscoverSchema(ctx context.Context, ps internal.PlanetScaleSource) (internal.Catalog, error) {
	return td.discoverSchemaResponse.catalog, td.discoverSchemaResponse.err
}

func (td testDatabase) Read(ctx context.Context, w io.Writer, ps internal.PlanetScaleSource, s internal.ConfiguredStream, tc *psdbconnect.TableCursor, lastCursor *internal.SerializedCursor) (*internal.SerializedCursor, error) {
	// TODO implement me
	panic("implement me")
}

func (td testDatabase) Close() error {
	return nil
}

func (td testDatabase) ListShards(ctx context.Context, ps internal.PlanetScaleSource) ([]string, error) {
	panic("implement me")
}
