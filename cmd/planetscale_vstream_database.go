package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

const READTIMEOUT = 15 * time.Second

type PlanetScaleVstreamDatabase struct {
}

type VGtidState struct {
	SerializedVGtids string `json:"vgtids"`
}

func (p PlanetScaleVstreamDatabase) CanConnect(ctx context.Context, psc PlanetScaleConnection) (bool, error) {
	return PlanetScaleMySQLDatabase{}.CanConnect(ctx, psc)
}

func (p PlanetScaleVstreamDatabase) DiscoverSchema(ctx context.Context, psc PlanetScaleConnection) (Catalog, error) {
	return PlanetScaleMySQLDatabase{}.DiscoverSchema(ctx, psc)
}

func (p PlanetScaleVstreamDatabase) Read(ctx context.Context, ps PlanetScaleConnection, s Stream, state string) error {
	//amsgChannel := make(chan AirbyteMessage)
	go p.readVGtidStream(ctx, state, s)
	select {

	case <-time.After(READTIMEOUT):
		return nil
	}
	return nil
}

func (p PlanetScaleVstreamDatabase) readVGtidStream(ctx context.Context, state string, s Stream) error {
	var (
		shardGtids []*binlogdatapb.ShardGtid
	)
	if state != "" {
		shardGtids, err := parseVGtidState(state)
		if err != nil {
			fmt.Printf("\nUnable to read state, failed with error : [%v]\n", err)
			return err
		}
		fmt.Printf("Found existing state, continuing where we left off, state len(%v) is [%v]", len(shardGtids), shardGtids)
	}

	if len(shardGtids) == 0 {
		shardGtids = []*binlogdatapb.ShardGtid{
			{
				Keyspace: s.Namespace,
				Shard:    "-",
				Gtid:     "",
			},
		}
	}

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: shardGtids,
	}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: s.Name,
		}},
	}

	conn, err := vtgateconn.Dial(ctx, "localhost:15999")
	if err != nil {
		log.Println("Failed dialing grpc connection")
		log.Fatal(err)
	}

	defer conn.Close()
	flags := &vtgatepb.VStreamFlags{}
	reader, err := conn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
	var fields []*querypb.Field
	var rowEvents []*binlogdatapb.RowEvent
outer:
	for {
		e, err := reader.Recv()

		if err != nil {
			fmt.Printf("remote error: %v at %v, retrying in 1s\n", err, vgtid)
			time.Sleep(1 * time.Second)
			continue outer
		}
		for _, event := range e {
			switch event.Type {
			case binlogdatapb.VEventType_FIELD:
				fields = event.FieldEvent.Fields
			case binlogdatapb.VEventType_ROW:
				rowEvents = append(rowEvents, event.RowEvent)
			case binlogdatapb.VEventType_VGTID:
				vgtid = event.Vgtid
			case binlogdatapb.VEventType_COMMIT:
				printRowEvents(vgtid, rowEvents, fields, s.Name)
				rowEvents = nil
			}
		}
	}
}
func parseVGtidState(state string) ([]*binlogdatapb.ShardGtid, error) {
	var vgs VGtidState
	err := json.Unmarshal([]byte(state), &vgs)
	if err != nil {
		return nil, err
	}
	var shardGtids []*binlogdatapb.ShardGtid
	err = json.Unmarshal([]byte(vgs.SerializedVGtids), &shardGtids)
	return shardGtids, err
}

func printRowEvents(vgtid *binlogdatapb.VGtid, rowEvents []*binlogdatapb.RowEvent, fields []*querypb.Field, tableName string) {

	for _, re := range rowEvents {
		result := &sqltypes.Result{
			Fields: fields,
		}

		for _, change := range re.RowChanges {
			typ := "U"
			if change.Before == nil {
				typ = "I"
			} else if change.After == nil {
				typ = "D"
			}

			switch typ {
			case "U", "I":
				p3qr := &querypb.QueryResult{
					Fields: fields,
					Rows:   []*querypb.Row{change.After},
				}
				qr := sqltypes.Proto3ToResult(p3qr)
				result.Rows = append(result.Rows, qr.Rows[0])
			case "D":
				// row was deleted, do nothing.
			}
		}
		printQueryResult(os.Stdout, result, tableName)
	}

	printVgtidState(os.Stdout, vgtid)
}

func printVgtidState(writer io.Writer, vgtid *binlogdatapb.VGtid) {
	b, _ := json.Marshal(vgtid)
	state := AirbyteMessage{
		Type:  STATE,
		State: &AirbyteState{map[string]string{"vgtids": string(b)}},
	}
	msg, _ := json.Marshal(state)
	fmt.Fprintf(writer, "%s\n", string(msg))
}

// printQueryResult will pretty-print an AirbyteRecordMessage to the logger.
// Copied from vtctl/query.go
func printQueryResult(writer io.Writer, qr *sqltypes.Result, tableName string) {
	var data = make(map[string]interface{})

	columns := make([]string, 0, len(qr.Fields))
	for _, field := range qr.Fields {
		columns = append(columns, field.Name)
	}
	now := time.Now()
	for _, row := range qr.Rows {
		for idx, val := range row {
			if idx < len(columns) {
				data[columns[idx]] = val
			}
		}
		rec := AirbyteMessage{
			Type: RECORD,
			Record: &AirbyteRecord{
				Stream:    tableName,
				Data:      data,
				EmittedAt: now.UnixMilli(),
			},
		}
		msg, _ := json.Marshal(rec)
		fmt.Fprintf(writer, "%s\n", string(msg))
	}
}
