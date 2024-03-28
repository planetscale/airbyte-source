package internal

import (
	"io"
	"sync"
	"time"

	"github.com/goccy/go-json"
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

const MaxBatchSize = 250000

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

	rMutex sync.Mutex
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

	a.rMutex.Lock()
	defer a.rMutex.Unlock()

	a.records = append(a.records, amsg)
	if len(a.records) == MaxBatchSize {
		a.flush()
	}
}

func (a *airbyteLogger) Flush() {
	a.rMutex.Lock()
	defer a.rMutex.Unlock()
	a.flush()
}

func (a *airbyteLogger) State(syncState SyncState) {
	a.recordEncoder.Encode(AirbyteMessage{
		Type:  STATE,
		State: &AirbyteState{syncState},
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

func (a *airbyteLogger) flush() {
	for _, record := range a.records {
		a.recordEncoder.Encode(record)
	}
	a.records = a.records[:0]
}

func preamble() string {
	return "PlanetScale Source :: "
}
