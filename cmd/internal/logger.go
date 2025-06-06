package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

type AirbyteLogger interface {
	Log(level, message string)
	Catalog(catalog Catalog)
	ConnectionStatus(status ConnectionStatus)
	Record(tableNamespace, tableName string, data map[string]interface{})
	Flush()
	State(syncState SyncState)
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
	if err := a.recordEncoder.Encode(AirbyteMessage{
		Type: LOG,
		Log: &AirbyteLogMessage{
			Level:   level,
			Message: preamble() + message,
		},
	}); err != nil {
		fmt.Fprintf(os.Stderr, "%sFailed to write log message: %v", preamble(), err)
	}
}

func (a *airbyteLogger) Catalog(catalog Catalog) {
	if err := a.recordEncoder.Encode(AirbyteMessage{
		Type:    CATALOG,
		Catalog: &catalog,
	}); err != nil {
		a.Error(fmt.Sprintf("catalog encoding error: %v", err))
	}
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
		if err := a.recordEncoder.Encode(record); err != nil {
			a.Error(fmt.Sprintf("flush encoding error: %v", err))
		}
	}
	a.records = a.records[:0]
}

func (a *airbyteLogger) State(syncState SyncState) {
	if err := a.recordEncoder.Encode(AirbyteMessage{
		Type:  STATE,
		State: &AirbyteState{syncState},
	}); err != nil {
		a.Error(fmt.Sprintf("state encoding error: %v", err))
	}
}

func (a *airbyteLogger) Error(error string) {
	if err := a.recordEncoder.Encode(AirbyteMessage{
		Type: LOG,
		Log: &AirbyteLogMessage{
			Level:   LOGLEVEL_ERROR,
			Message: error,
		},
	}); err != nil {
		fmt.Fprintf(os.Stderr, "%sFailed to write error: %v", preamble(), err)
	}
}

func (a *airbyteLogger) ConnectionStatus(status ConnectionStatus) {
	if err := a.recordEncoder.Encode(AirbyteMessage{
		Type:             CONNECTION_STATUS,
		ConnectionStatus: &status,
	}); err != nil {
		a.Error(fmt.Sprintf("connection status encoding error: %v", err))
	}
}

func preamble() string {
	return "PlanetScale Source :: "
}
