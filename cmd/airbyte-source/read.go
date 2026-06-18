package airbyte_source

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/planetscale/airbyte-source/cmd/internal"
	psdbconnectv1alpha1 "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/spf13/cobra"
)

var (
	readSourceConfigFilePath string
	readSourceCatalogPath    string
	stateFilePath            string
)

func init() {
	rootCmd.AddCommand(ReadCommand(DefaultHelper(os.Stdout)))
}

func ReadCommand(ch *Helper) *cobra.Command {
	readCmd := &cobra.Command{
		Use:          "read",
		Short:        "Converts rows from a PlanetScale database into AirbyteRecordMessages",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			ch.Logger = internal.NewLogger(cmd.OutOrStdout())
			if readSourceConfigFilePath == "" {
				fmt.Fprintf(cmd.ErrOrStderr(), "Please pass path to a valid source config file via the [%v] argument", "config")
				return fmt.Errorf("missing config file path")
			}

			if readSourceCatalogPath == "" {
				fmt.Fprintf(cmd.OutOrStdout(), "Please pass path to a valid source catalog file via the [%v] argument", "config")
				return fmt.Errorf("missing catalog file path")
			}

			psc, err := parseSource(ch.FileReader, readSourceConfigFilePath)
			if err != nil {
				fmt.Fprintln(cmd.OutOrStdout(), "Please provide path to a valid configuration file")
				return err
			}

			ch.Logger.Log(internal.LOGLEVEL_INFO, "Ensure database")
			if err := ch.EnsureDB(psc); err != nil {
				fmt.Fprintln(cmd.OutOrStdout(), "Unable to connect to PlanetScale Database")
				return err
			}

			defer func() {
				if err := ch.Database.Close(); err != nil {
					fmt.Fprintf(cmd.OutOrStdout(), "Unable to close connection to PlanetScale Database, failed with %v", err)
				}
			}()

			ch.Logger.Log(internal.LOGLEVEL_INFO, "Checking connection")
			cs, err := checkConnectionStatus(ctx, ch.Database, psc)
			if err != nil {
				ch.Logger.ConnectionStatus(cs)
				return err
			}

			ch.Logger.Log(internal.LOGLEVEL_INFO, "Reading catalog")
			catalog, err := readCatalog(readSourceCatalogPath)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to read catalog: %+v", err))
				return fmt.Errorf("unable to read catalog: %w", err)
			}

			if len(catalog.Streams) == 0 {
				ch.Logger.Log(internal.LOGLEVEL_ERROR, "Catalog has no streams")
				return nil
			}

			state := ""
			if stateFilePath != "" {
				ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("State file detected, parsing provided file %s", stateFilePath))
				b, err := os.ReadFile(stateFilePath)
				if err != nil {
					ch.Logger.Error(fmt.Sprintf("Unable to read state : %v", err))
					return fmt.Errorf("unable to read state: %w", err)
				}
				state = string(b)
			}

			ch.Logger.Log(internal.LOGLEVEL_INFO, "Listing shards")
			shards, err := ch.Database.ListShards(ctx, psc)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to list shards : %v", err))
				return fmt.Errorf("unable to list shards: %w", err)
			}

			ch.Logger.Log(internal.LOGLEVEL_INFO, "Reading state")
			syncState, err := readState(state, psc, catalog.Streams, shards, ch.Logger)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to read state : %v", err))
				return fmt.Errorf("unable to read state: %w", err)
			}

			var readErr error
			for _, configuredStream := range catalog.Streams {
				keyspaceOrDatabase, streamStateKey := streamStateKeyFor(configuredStream.Stream.Namespace, configuredStream.Stream.Name, psc.Database)
				streamState, ok := syncState.Streams[streamStateKey]
				if !ok {
					ch.Logger.Error(fmt.Sprintf("Unable to read state for stream %v", streamStateKey))
					ch.Logger.StreamStatus(keyspaceOrDatabase, configuredStream.Stream.Name, internal.STREAM_STATUS_INCOMPLETE)
					return fmt.Errorf("unable to read state for stream %v", streamStateKey)
				}

				ch.Logger.StreamStatus(keyspaceOrDatabase, configuredStream.Stream.Name, internal.STREAM_STATUS_STARTED)

				streamFailed := false
				for shardName, shardState := range streamState.Shards {
					var tc *psdbconnectv1alpha1.TableCursor

					tc, err = shardState.SerializedCursorToTableCursor(configuredStream)
					ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Using serialized cursor for stream %s", streamStateKey))
					if err != nil {
						ch.Logger.Error(fmt.Sprintf("Invalid serialized cursor for stream %v, failed with [%v]", streamStateKey, err))
						streamFailed = true
						// A bad cursor only affects this shard; keep going so the
						// other shards in this stream can still sync.
						continue
					}

					sc, err := ch.Database.Read(ctx, cmd.OutOrStdout(), psc, configuredStream, tc)
					// Read can return a cursor reflecting the progress made so far
					// alongside an error (e.g. on a server timeout), so persist it
					// before handling the error to avoid re-reading already-synced
					// data on the next attempt.
					if sc != nil {
						syncState.Streams[streamStateKey].Shards[shardName] = sc
					}
					if err != nil {
						ch.Logger.Error(err.Error())
						streamFailed = true
						// One shard failing shouldn't stop the others from syncing.
						continue
					}
				}

				// Always emit state to checkpoint whatever progress was made,
				// including partial progress when only some shards succeeded.
				ch.Logger.StreamState(keyspaceOrDatabase, configuredStream.Stream.Name, syncState.Streams[streamStateKey])

				if streamFailed {
					ch.Logger.StreamStatus(keyspaceOrDatabase, configuredStream.Stream.Name, internal.STREAM_STATUS_INCOMPLETE)
					readErr = fmt.Errorf("read failed for stream %v", streamStateKey)
				} else {
					ch.Logger.StreamStatus(keyspaceOrDatabase, configuredStream.Stream.Name, internal.STREAM_STATUS_COMPLETE)
				}
			}

			return readErr
		},
	}
	readCmd.Flags().StringVar(&readSourceCatalogPath, "catalog", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&readSourceConfigFilePath, "config", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&stateFilePath, "state", "", "Path to the PlanetScale state information")
	return readCmd
}

type State struct {
	Shards map[string]map[string]interface{} `json:"shards"`
}

// streamStateKeyFor resolves the effective namespace for a stream (defaulting
// to the source database when the catalog leaves it empty) and the composite
// key used to look that stream up in the sync state. Keeping this in one place
// avoids the namespace/key logic drifting between the read loop and readState.
func streamStateKeyFor(namespace, streamName, database string) (string, string) {
	if namespace == "" {
		namespace = database
	}
	return namespace, namespace + ":" + streamName
}

func readState(state string, psc internal.PlanetScaleSource, streams []internal.ConfiguredStream, shards []string, logger internal.AirbyteLogger) (internal.SyncState, error) {
	syncState := internal.SyncState{
		Streams: map[string]internal.ShardStates{},
	}
	if state != "" {
		// Try parsing as Airbyte v2 per-stream state array first
		var perStreamStates []internal.AirbyteState
		if err := json.Unmarshal([]byte(state), &perStreamStates); err == nil && len(perStreamStates) > 0 && perStreamStates[0].Type == internal.STATE_TYPE_STREAM {
			logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Parsing Airbyte v2 per-stream state (%d streams)", len(perStreamStates)))
			for _, s := range perStreamStates {
				if s.Stream != nil && s.Stream.StreamState != nil {
					_, key := streamStateKeyFor(s.Stream.StreamDescriptor.Namespace, s.Stream.StreamDescriptor.Name, psc.Database)
					syncState.Streams[key] = *s.Stream.StreamState
				}
			}
		} else {
			// Fall back to legacy global state format
			err := json.Unmarshal([]byte(state), &syncState)
			if err != nil {
				return syncState, err
			}
		}
	}

	for _, s := range streams {
		keyspaceOrDatabase, stateKey := streamStateKeyFor(s.Stream.Namespace, s.Stream.Name, psc.Database)
		logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Syncing stream %s with sync mode %s", s.Stream.Name, s.SyncMode))
		ignoreCurrentCursor := !s.IncrementalSyncRequested()

		// if no table cursor was found in the state, or we want to ignore the current cursor,
		// Send along an empty cursor for each shard.
		if _, ok := syncState.Streams[stateKey]; !ok || ignoreCurrentCursor {
			logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Ignoring current cursor since incremental sync is disabled, or no cursor was found for key %s", stateKey))
			initialState, err := psc.GetInitialState(keyspaceOrDatabase, shards)
			if err != nil {
				return syncState, err
			}
			syncState.Streams[stateKey] = initialState
		}
	}

	return syncState, nil
}

func readCatalog(path string) (c internal.ConfiguredCatalog, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(b, &c)
	return c, err
}
