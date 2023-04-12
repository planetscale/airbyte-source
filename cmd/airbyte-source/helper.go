package airbyte_source

import (
	"io"
	"os"

	"github.com/planetscale/airbyte-source/cmd/internal"
	"github.com/planetscale/airbyte-source/lib"
)

type Helper struct {
	Connect    lib.ConnectClient
	Mysql      lib.MysqlClient
	FileReader FileReader
	Logger     internal.AirbyteSerializer
}

type FileReader interface {
	ReadFile(path string) ([]byte, error)
}

type fileReader struct{}

func (f fileReader) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func DefaultHelper(w io.Writer) *Helper {
	logger := internal.NewSerializer(w)
	return &Helper{
		FileReader: fileReader{},
		Logger:     logger,
	}
}

func (h *Helper) EnsureConnect(psc lib.PlanetScaleSource) error {
	if h.Connect != nil {
		return nil
	}

	var err error
	h.Mysql, err = lib.NewMySQL(&psc)
	if err != nil {
		return err
	}
	h.Connect = lib.NewConnectClient(&h.Mysql)
	return nil
}
