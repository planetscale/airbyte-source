package airbyte_source

import (
	"github.com/planetscale/connect/source/cmd/internal"
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

func DefaultHelper() *Helper {
	logger := internal.NewLogger()
	return &Helper{
		Database: internal.PlanetScaleVstreamDatabase{
			Logger: logger,
		},
		FileReader: fileReader{},
		Logger:     logger,
	}
}
