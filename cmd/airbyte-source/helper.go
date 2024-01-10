package airbyte_source

import (
	"github.com/planetscale/airbyte-source/cmd/internal"
	"github.com/planetscale/connect-sdk/lib"
	"io"
	"os"
)

type Helper struct {
	MysqlClient   lib.MysqlClient
	ConnectClient lib.ConnectClient
	Source        lib.PlanetScaleSource
	FileReader    FileReader
	Logger        internal.AirbyteLogger
}

type FileReader interface {
	ReadFile(path string) ([]byte, error)
}

type fileReader struct{}

func (f fileReader) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func DefaultHelper(w io.Writer) *Helper {
	logger := internal.NewLogger(w)
	return &Helper{
		FileReader: fileReader{},
		Logger:     logger,
	}
}

func (h *Helper) EnsureDB(psc internal.PlanetScaleSource) error {
	if h.ConnectClient != nil {
		return nil
	}

	h.Source = lib.PlanetScaleSource{
		UseReplica: true,
		Username:   psc.Username,
		Database:   psc.Database,
		Host:       psc.Host,
		Password:   psc.Password,
	}
	var err error
	h.MysqlClient, err = lib.NewMySQL(&h.Source)
	if err != nil {
		return err
	}
	h.ConnectClient = lib.NewConnectClient(&h.MysqlClient)

	return nil
}
