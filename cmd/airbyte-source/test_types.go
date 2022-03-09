package airbyte_source

import (
	"context"
	"github.com/planetscale/connect/source/cmd/internal"
	"io"
)

type testFileReader struct {
	content []byte
	err     error
}

func (tfr testFileReader) ReadFile(path string) ([]byte, error) {
	return tfr.content, tfr.err
}

type canConnectResponse struct {
	canConnect bool
	err        error
}

type discoverSchemaResponse struct {
	catalog internal.Catalog
	err     error
}

type testDatabase struct {
	connectResponse        canConnectResponse
	discoverSchemaResponse discoverSchemaResponse
}

func (td testDatabase) CanConnect(ctx context.Context, ps internal.PlanetScaleConnection) (bool, error) {
	return td.connectResponse.canConnect, td.connectResponse.err
}

func (td testDatabase) DiscoverSchema(ctx context.Context, ps internal.PlanetScaleConnection) (internal.Catalog, error) {
	return td.discoverSchemaResponse.catalog, td.discoverSchemaResponse.err
}

func (td testDatabase) Read(ctx context.Context, w io.Writer, ps internal.PlanetScaleConnection, s internal.Stream, state string) error {
	//TODO implement me
	panic("implement me")
}