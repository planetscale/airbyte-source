package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type AirbyteLogger interface {
	Log(w io.Writer, level, message string)
	Catalog(w io.Writer, catalog Catalog)
	ConnectionStatus(w io.Writer, status ConnectionStatus)
	Record(w io.Writer, tableNamespace, tableName string, data map[string]interface{})
	State(w io.Writer, data map[string]string)
	Error(w io.Writer, error string)
}

func NewLogger() AirbyteLogger {
	return airbyteLogger{}
}

type airbyteLogger struct{}

func (a airbyteLogger) Log(w io.Writer, level, message string) {
	msg, _ := json.Marshal(AirbyteMessage{
		Type: LOG,
		Log: &AirbyteLogMessage{
			Level:   level,
			Message: message,
		},
	})

	fmt.Fprintf(w, "%s\n", msg)
}

func (a airbyteLogger) Spec(w io.Writer, spec Spec) {

}

func (a airbyteLogger) Catalog(w io.Writer, catalog Catalog) {
	msg, _ := json.Marshal(AirbyteMessage{
		Type:    CATALOG,
		Catalog: &catalog,
	})

	fmt.Fprintf(w, "%s\n", msg)
}

func (a airbyteLogger) Record(w io.Writer, tableNamespace, tableName string, data map[string]interface{}) {
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

	msg, _ := json.Marshal(amsg)
	fmt.Fprintf(w, "%s\n", msg)
}

func (a airbyteLogger) State(w io.Writer, data map[string]string) {
	state := AirbyteMessage{
		Type:  STATE,
		State: &AirbyteState{data},
	}
	msg, _ := json.Marshal(state)
	fmt.Fprintf(w, "%s\n", string(msg))
}

func (a airbyteLogger) Error(w io.Writer, error string) {
	logline := AirbyteMessage{
		Type: LOG,
		Log: &AirbyteLogMessage{
			Level:   LOGLEVEL_ERROR,
			Message: error,
		},
	}
	msg, _ := json.Marshal(logline)
	fmt.Fprintf(w, "%s\n", string(msg))
}

func (a airbyteLogger) ConnectionStatus(w io.Writer, status ConnectionStatus) {
	msg, _ := json.Marshal(AirbyteMessage{
		Type:             CONNECTION_STATUS,
		ConnectionStatus: &status,
	})
	fmt.Fprintf(w, "%s\n", string(msg))
}
