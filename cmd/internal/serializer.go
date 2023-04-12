package internal

import (
	"encoding/json"
	"io"
	"time"

	"github.com/planetscale/airbyte-source/lib"
)

type AirbyteSerializer interface {
	Info(message string)
	Log(level, message string)
	Catalog(catalog Catalog)
	ConnectionStatus(status ConnectionStatus)
	Record(tableNamespace, tableName string, data map[string]interface{})
	Flush()
	State(syncState lib.SyncState)
	Error(error string)
}

const MaxBatchSize = 10000

func NewSerializer(w io.Writer) AirbyteSerializer {
	al := airbyteSerializer{}
	al.writer = w
	al.recordEncoder = json.NewEncoder(w)
	al.records = make([]AirbyteMessage, 0, MaxBatchSize)
	return &al
}

type airbyteSerializer struct {
	recordEncoder *json.Encoder
	writer        io.Writer
	records       []AirbyteMessage
}

func (a *airbyteSerializer) Info(message string) {
	a.Log(LOGLEVEL_INFO, message)
}

func (a *airbyteSerializer) Log(level, message string) {
	a.recordEncoder.Encode(AirbyteMessage{
		Type: LOG,
		Log: &AirbyteLogMessage{
			Level:   level,
			Message: preamble() + message,
		},
	})
}

func (a *airbyteSerializer) Catalog(catalog Catalog) {
	a.recordEncoder.Encode(AirbyteMessage{
		Type:    CATALOG,
		Catalog: &catalog,
	})
}

func (a *airbyteSerializer) Record(tableNamespace, tableName string, data map[string]interface{}) {
	now := time.Now()
	amsg := AirbyteMessage{
		Type: RECORD,
		Record: &AirbyteRecord{
			Namespace: tableNamespace,
			Stream:    tableName,
			Data:      data,
			EmittedAt: now.UnixMilli(),
		},
	}

	a.records = append(a.records, amsg)
	if len(a.records) == MaxBatchSize {
		a.Flush()
	}
}

func (a *airbyteSerializer) Flush() {
	for _, record := range a.records {
		a.recordEncoder.Encode(record)
	}
	a.records = a.records[:0]
}

func (a *airbyteSerializer) State(syncState lib.SyncState) {
	a.recordEncoder.Encode(AirbyteMessage{
		Type:  STATE,
		State: &AirbyteState{syncState},
	})
}

func (a *airbyteSerializer) Error(error string) {
	a.recordEncoder.Encode(AirbyteMessage{
		Type: LOG,
		Log: &AirbyteLogMessage{
			Level:   LOGLEVEL_ERROR,
			Message: error,
		},
	})
}

func (a *airbyteSerializer) ConnectionStatus(status ConnectionStatus) {
	a.recordEncoder.Encode(AirbyteMessage{
		Type:             CONNECTION_STATUS,
		ConnectionStatus: &status,
	})
}

func preamble() string {
	return "PlanetScale Source :: "
}
