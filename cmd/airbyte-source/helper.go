package airbyte_source

import (
	"github.com/planetscale/connect/source/cmd/internal"
	"io"
	"io/ioutil"
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
		Database: internal.PlanetScaleEdgeDatabase{
			Logger: logger,
		},
		FileReader: fileReader{},
		Logger:     logger,
	}
}
