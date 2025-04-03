package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
)

type AirbyteLogger interface {
	Log(level, message string)
	Catalog(catalog Catalog)
	ConnectionStatus(status ConnectionStatus)
	Record(tableNamespace, tableName string, data map[string]interface{}) error
	Flush() error
	State(syncState SyncState)
	Error(error string)
	QueueFull() bool
}

const MaxBatchSize = 10000

var (
	errExceededMaxRecordBatchSize = errors.New("exceeded max record batch size")

	_ AirbyteLogger = (*airbyteLogger)(nil)
)

func NewLogger(w io.Writer) AirbyteLogger {
	buffer := &bytes.Buffer{}
	return &airbyteLogger{
		buffer:  buffer,
		encoder: json.NewEncoder(buffer),
		records: make([]AirbyteMessage, 0, MaxBatchSize),
		writer:  w,
	}
}

type airbyteLogger struct {
	buffer  *bytes.Buffer
	encoder *json.Encoder
	writer  io.Writer
	records []AirbyteMessage
}

// QueueFull implements AirbyteLogger.
func (a *airbyteLogger) QueueFull() bool {
	return len(a.records) >= MaxBatchSize
}

func (a *airbyteLogger) Log(level, message string) {
	if err := a.encodeAndWriteMessages([]AirbyteMessage{{
		Type: LOG,
		Log: &AirbyteLogMessage{
			Level:   level,
			Message: preamble() + message,
		},
	}}); err != nil {
		// TODO: return this as an error
		fmt.Fprintf(os.Stderr, "%sFailed to write log message: %v", preamble(), err)
	}
}

func (a *airbyteLogger) Catalog(catalog Catalog) {
	if err := a.encodeAndWriteMessages([]AirbyteMessage{{
		Type:    CATALOG,
		Catalog: &catalog,
	}}); err != nil {
		// TODO: return this as an error
		a.Error(fmt.Sprintf("catalog encoding error: %v", err))
	}
}

func (a *airbyteLogger) Record(tableNamespace, tableName string, data map[string]interface{}) error {
	if len(a.records) >= MaxBatchSize {
		return errExceededMaxRecordBatchSize
	}

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
	return nil
}

func (a *airbyteLogger) Flush() error {
	if len(a.records) == 0 {
		return nil
	}
	if err := a.encodeAndWriteMessages(a.records); err != nil {
		return fmt.Errorf("encode and write messages: %w", err)
	}
	a.records = a.records[:0]
	return nil
}

func (a *airbyteLogger) State(syncState SyncState) {
	if err := a.encodeAndWriteMessages([]AirbyteMessage{{
		Type:  STATE,
		State: &AirbyteState{syncState},
	}}); err != nil {
		// TODO: return this as an error
		a.Error(fmt.Sprintf("state encoding error: %v", err))
	}
}

func (a *airbyteLogger) Error(error string) {
	if err := a.encodeAndWriteMessages([]AirbyteMessage{{
		Type: LOG,
		Log: &AirbyteLogMessage{
			Level:   LOGLEVEL_ERROR,
			Message: error,
		},
	}}); err != nil {
		// TODO: return this as an error
		fmt.Fprintf(os.Stderr, "%sFailed to write error: %v", preamble(), err)
	}
}

func (a *airbyteLogger) ConnectionStatus(status ConnectionStatus) {
	if err := a.encodeAndWriteMessages([]AirbyteMessage{{
		Type:             CONNECTION_STATUS,
		ConnectionStatus: &status,
	}}); err != nil {
		// TODO: return this as an error
		a.Error(fmt.Sprintf("connection status encoding error: %v", err))
	}
}

func (a *airbyteLogger) encodeAndWriteMessages(ms []AirbyteMessage) error {
	if len(ms) == 0 {
		return nil
	}

	defer a.buffer.Reset()

	for _, m := range ms {
		if err := a.encoder.Encode(m); err != nil {
			return fmt.Errorf("encode: %w", err)
		}
	}

	if _, err := a.writer.Write(a.buffer.Bytes()); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	return nil
}

func preamble() string {
	return "PlanetScale Source :: "
}
