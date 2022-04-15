package internal

import (
	"encoding/json"
	"io"
	"time"
)

type AirbyteLogger interface {
	Log(level, message string)
	Catalog(catalog Catalog)
	ConnectionStatus(status ConnectionStatus)
	Record(tableNamespace, tableName string, data map[string]interface{})
	Flush()
	State(data map[string]interface{})
	Error(error string)
}

const MaxBatchSize = 10000

func NewLogger(w io.Writer) AirbyteLogger {
	al := airbyteLogger{}
	al.writer = w
	al.recordEncoder = json.NewEncoder(w)
	al.records = make([]AirbyteMessage, 0, MaxBatchSize)
	return &al
}

type airbyteLogger struct {
	recordEncoder *json.Encoder
	writer        io.Writer
	records       []AirbyteMessage
}

func (a *airbyteLogger) Log(level, message string) {
	a.recordEncoder.Encode(AirbyteMessage{
		Type: LOG,
		Log: &AirbyteLogMessage{
			Level:   level,
			Message: preamble() + message,
		},
	})
}

func (a *airbyteLogger) Spec(spec Spec) {

}

func (a *airbyteLogger) Catalog(catalog Catalog) {
	a.recordEncoder.Encode(AirbyteMessage{
		Type:    CATALOG,
		Catalog: &catalog,
	})
}

func (a *airbyteLogger) Record(tableNamespace, tableName string, data map[string]interface{}) {
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

func (a *airbyteLogger) Flush() {
	for _, record := range a.records {
		a.recordEncoder.Encode(record)
	}
	a.records = a.records[:0]
}

func (a *airbyteLogger) State(data map[string]interface{}) {
	a.recordEncoder.Encode(AirbyteMessage{
		Type:  STATE,
		State: &AirbyteState{data},
	})
}

func (a *airbyteLogger) Error(error string) {
	a.recordEncoder.Encode(AirbyteMessage{
		Type: LOG,
		Log: &AirbyteLogMessage{
			Level:   LOGLEVEL_ERROR,
			Message: error,
		},
	})
}

func (a *airbyteLogger) ConnectionStatus(status ConnectionStatus) {
	a.recordEncoder.Encode(AirbyteMessage{
		Type:             CONNECTION_STATUS,
		ConnectionStatus: &status,
	})
}

func preamble() string {
	return "PlanetScale Source :: "
}
