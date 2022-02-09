package cmd

type ConnectionStatus struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

const (
	RECORD            = "RECORD"
	STATE             = "STATE"
	LOG               = "LOG"
	SPEC              = "SPEC"
	CONNECTION_STATUS = "CONNECTION_STATUS"
	CATALOG           = "CATALOG"
)

const (
	LOGLEVEL_FATAL    = "FATAL"
	LOGLEVEL_CRITICAL = "CRITICAL"
	LOGLEVEL_ERROR    = "ERROR"
	LOGLEVEL_WARN     = "WARN"
	LOGLEVEL_WARNING  = "WARNING"
	LOGLEVEL_INFO     = "INFO"
	LOGLEVEL_DEBUG    = "DEBUG"
	LOGLEVEL_TRACE    = "TRACE"
)

type AirbyteLogMessage struct {
	Level   string `json:"level,omitempty"`
	Message string `json:"message,omitempty"`
}

const (
	SYNC_MODE_FULL_REFRESH = "full_refresh"
	SYNC_MODE_INCREMENTAL  = "incremental"
)

type PropertyType struct {
	Type string `json:"type"`
}

type StreamSchema struct {
	Type       string                  `json:"type""`
	Properties map[string]PropertyType `json:"properties"`
}

type Catalog struct {
	Streams []Stream `json:"streams"`
}

type ConfiguredStream struct {
	Stream Stream `json:"stream"`
}

type ConfiguredCatalog struct {
	Streams []ConfiguredStream `json:"streams"`
}

type AirbyteRecord struct {
	Stream    string            `json:"stream"`
	EmittedAt int64             `json:"emitted_at"`
	Data      map[string]string `json:"data"`
}

type AirbyteState struct {
	Data map[string]string `json:"data"`
}

type AirbyteMessage struct {
	Type             string             `json:"type"`
	Log              *AirbyteLogMessage `json:"log,omitempty"`
	ConnectionStatus *ConnectionStatus  `json:"connectionStatus,omitempty"`
	Catalog          *Catalog           `json:"catalog,omitempty"`
	Record           *AirbyteRecord     `json:"record,omitempty"`
	State            *AirbyteState      `json:"state,omitempty"`
}
