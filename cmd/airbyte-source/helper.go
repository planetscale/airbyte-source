package airbyte_source

import (
	"io"
	"io/ioutil"

	"github.com/planetscale/airbyte-source/cmd/internal"
)

type Helper struct {
	Database   internal.PlanetScaleDatabase
	FileReader FileReader
	Logger     internal.AirbyteLogger
}

type FileReader interface {
	ReadFile(path string) ([]byte, error)
}

type fileReader struct{}

func (f fileReader) ReadFile(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}

func DefaultHelper(w io.Writer) *Helper {
	logger := internal.NewLogger(w)
	return &Helper{
		FileReader: fileReader{},
		Logger:     logger,
	}
}

func (h *Helper) EnsureDB(psc internal.PlanetScaleSource) error {
	if h.Database != nil {
		return nil
	}

	mysql, err := internal.NewMySQL(&psc)
	if err != nil {
		return err
	}
	h.Database = internal.NewPlanetScaleEdgeDatabase(h.Logger, mysql)
	return nil
}
