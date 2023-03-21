package airbyte_source

import (
	"github.com/planetscale/airbyte-source/cmd/types"
	"github.com/planetscale/airbyte-source/shared"
	"io"
	"os"
)

type Helper struct {
	Database     shared.PlanetScaleDatabase
	SchemaClient shared.PlanetScaleDatabaseSchemaClient
	FileReader   FileReader
	Logger       types.AirbyteLogger
}

type FileReader interface {
	ReadFile(path string) ([]byte, error)
}

type fileReader struct{}

func (f fileReader) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func DefaultHelper(w io.Writer) *Helper {
	logger := types.NewLogger(w)
	return &Helper{
		FileReader: fileReader{},
		Logger:     logger,
	}
}

func (h *Helper) EnsureDB(psc types.PlanetScaleSource) error {
	if h.Database != nil {
		return nil
	}

	mysql, err := shared.NewMySQL(&psc)
	if err != nil {
		return err
	}
	h.Database = shared.PlanetScaleEdgeDatabase{
		Logger: h.Logger,
		Mysql:  mysql,
	}

	h.SchemaClient = shared.PlanetScaleEdgeDatabaseSchema{
		Logger: h.Logger,
		Mysql:  mysql,
	}

	return nil
}
