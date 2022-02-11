package cmd

import "io/ioutil"

type Helper struct {
	Database   IPlanetScaleDatabase
	FileReader IFileReader
}

type IFileReader interface {
	ReadFile(path string) ([]byte, error)
}

type FileReader struct{}

func (f FileReader) ReadFile(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}

func DefaultHelper() *Helper {
	return &Helper{
		Database:   PlanetScaleMySQLDatabase{},
		FileReader: FileReader{},
	}
}
