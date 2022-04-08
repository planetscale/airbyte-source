package internal

import (
	"encoding/json"
	"fmt"
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

const MAX_BATCH_SIZE = 1000

func NewLogger(w io.Writer) AirbyteLogger {
	al := airbyteLogger{}
	al.writer = w
	al.recordEncoder = json.NewEncoder(w)
	al.records = make([]AirbyteMessage, 0, MAX_BATCH_SIZE)
	return &al
}

type airbyteLogger struct {
	recordEncoder *json.Encoder
	writer        io.Writer
	records       []AirbyteMessage
}

func (a *airbyteLogger) Log(level, message string) {
	msg, _ := json.Marshal(AirbyteMessage{
		Type: LOG,
		Log: &AirbyteLogMessage{
			Level:   level,
			Message: message,
		},
	})

	fmt.Fprintf(a.writer, "%s\n", msg)
}

func (a *airbyteLogger) Spec(spec Spec) {

}

func (a *airbyteLogger) Catalog(catalog Catalog) {
	msg, _ := json.Marshal(AirbyteMessage{
		Type:    CATALOG,
		Catalog: &catalog,
	})

	fmt.Fprintf(a.writer, "%s\n", msg)
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
	if len(a.records) == MAX_BATCH_SIZE {
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
	state := AirbyteMessage{
		Type:  STATE,
		State: &AirbyteState{data},
	}
	msg, _ := json.Marshal(state)
	fmt.Fprintf(a.writer, "%s\n", string(msg))
}

func (a *airbyteLogger) Error(error string) {
	logline := AirbyteMessage{
		Type: LOG,
		Log: &AirbyteLogMessage{
			Level:   LOGLEVEL_ERROR,
			Message: error,
		},
	}
	msg, _ := json.Marshal(logline)
	fmt.Fprintf(a.writer, "%s\n", string(msg))
}

func (a *airbyteLogger) ConnectionStatus(status ConnectionStatus) {
	msg, _ := json.Marshal(AirbyteMessage{
		Type:             CONNECTION_STATUS,
		ConnectionStatus: &status,
	})
	fmt.Fprintf(a.writer, "%s\n", string(msg))
}
