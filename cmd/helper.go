package cmd

import "io/ioutil"

type Helper struct {
	Database   PlanetScaleDatabase
	FileReader FileReader
	Logger     AirbyteLogger
}

type FileReader interface {
	ReadFile(path string) ([]byte, error)
}

type fileReader struct{}

func (f fileReader) ReadFile(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}

func DefaultHelper() *Helper {
	return &Helper{
		Database:   PlanetScaleMySQLDatabase{},
		FileReader: fileReader{},
		Logger:     NewLogger(),
	}
}
