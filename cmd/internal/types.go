package internal

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
	Stream   Stream `json:"stream"`
	SyncMode string `json:"sync_mode"`
}

func (cs ConfiguredStream) IncrementalSyncRequested() bool {
	return cs.SyncMode == "incremental"
}

func (cs ConfiguredStream) ResetRequested() bool {
	return cs.SyncMode == "append"
}

type ConfiguredCatalog struct {
	Streams []ConfiguredStream `json:"streams"`
}

type AirbyteRecord struct {
	Stream    string                 `json:"stream"`
	Namespace string                 `json:"namespace"`
	EmittedAt int64                  `json:"emitted_at"`
	Data      map[string]interface{} `json:"data"`
}

type AirbyteState struct {
	Data map[string]interface{} `json:"data"`
}

type AirbyteMessage struct {
	Type             string             `json:"type"`
	Log              *AirbyteLogMessage `json:"log,omitempty"`
	ConnectionStatus *ConnectionStatus  `json:"connectionStatus,omitempty"`
	Catalog          *Catalog           `json:"catalog,omitempty"`
	Record           *AirbyteRecord     `json:"record,omitempty"`
	State            *AirbyteState      `json:"state,omitempty"`
}

type SpecMessage struct {
	Type string `json:"type"`
	Spec Spec   `json:"spec"`
}

type ConnectionProperties struct {
	Host     ConnectionProperty `json:"host"`
	Database ConnectionProperty `json:"database"`
	Username ConnectionProperty `json:"username"`
	Password ConnectionProperty `json:"password"`
}

type ConnectionProperty struct {
	Description string `json:"description"`
	Title       string `json:"title"`
	Type        string `json:"type"`
	Order       int    `json:"order"`
	IsSecret    bool   `json:"airbyte_secret"`
}

type ConnectionSpecification struct {
	Schema               string               `json:"$schema"`
	Title                string               `json:"title"`
	Type                 string               `json:"type"`
	Required             []string             `json:"required"`
	AdditionalProperties bool                 `json:"additionalProperties"`
	Properties           ConnectionProperties `json:"properties"`
}

type Spec struct {
	DocumentationURL              string                  `json:"documentationUrl"`
	ConnectionSpecification       ConnectionSpecification `json:"connectionSpecification"`
	SupportsIncremental           bool                    `json:"supportsIncremental"`
	SupportsNormalization         bool                    `json:"supportsNormalization"`
	SupportsDBT                   bool                    `json:"supportsDBT"`
	SupportedDestinationSyncModes []string                `json:"supported_destination_sync_modes"`
}
