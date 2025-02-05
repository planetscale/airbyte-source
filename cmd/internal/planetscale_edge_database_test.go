package internal

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"
)

func TestRead_CanPeekBeforeRead(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger(b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	vstreamSyncClient := &vtgateVStreamClientMock{
		vstreamResponses: []*vtgate.VStreamResponse{
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    "-",
									Gtid:     "THIS_IS_A_SHARD_GTID",
									Keyspace: "connect-test",
								},
								{
									Shard:    "-",
									Gtid:     "THIS_IS_A_SHARD_GTID",
									Keyspace: "connect-test",
								},
							},
						},
					},
				},
			},
		},
	}

	vsc := vstreamClientMock{
		vstreamFn: func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
			return vstreamSyncClient, nil
		},
	}

	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error) {
		return &vsc, nil
	}

	ps := PlanetScaleSource{}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, vsc.vstreamFnInvokedCount)
	assert.False(t, tma.PingContextFnInvoked)
	assert.False(t, tma.GetVitessTabletsFnInvoked)
}

func TestRead_CanEarlyExitIfNoNewVGtidInPeek(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger(b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	vstreamSyncClient := &vtgateVStreamClientMock{
		vstreamResponses: []*vtgate.VStreamResponse{
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    "-",
									Gtid:     "THIS_IS_A_SHARD_GTID",
									Keyspace: "connect-test",
								},
								{
									Shard:    "-",
									Gtid:     "THIS_IS_A_SHARD_GTID",
									Keyspace: "connect-test",
								},
							},
						},
					},
				},
			},
		},
	}

	vsc := vstreamClientMock{
		vstreamFn: func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
			return vstreamSyncClient, nil
		},
	}

	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error) {
		return &vsc, nil
	}

	ps := PlanetScaleSource{}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, vsc.vstreamFnInvokedCount)
}

func TestRead_CanPickPrimaryForShardedKeyspaces(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger(b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "40-80",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	vstreamSyncClient := &vtgateVStreamClientMock{
		vstreamResponses: []*vtgate.VStreamResponse{
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    tc.Shard,
									Gtid:     tc.Position,
									Keyspace: tc.Keyspace,
								},
							},
						},
					},
				},
			},
		},
	}

	vsc := vstreamClientMock{
		vstreamFn: func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
			assert.Equal(t, topodata.TabletType_PRIMARY, in.TabletType)
			assert.Contains(t, in.Flags.Cells, "planetscale_operator_default")
			return vstreamSyncClient, nil
		},
	}

	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error) {
		return &vsc, nil
	}

	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, vsc.vstreamFnInvokedCount)
	assert.False(t, tma.PingContextFnInvoked)
	assert.False(t, tma.GetVitessTabletsFnInvoked)
}

func TestRead_CanPickReplicaForShardedKeyspaces(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger(b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "40-80",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	vstreamSyncClient := &vtgateVStreamClientMock{
		vstreamResponses: []*vtgate.VStreamResponse{
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    tc.Shard,
									Gtid:     tc.Position,
									Keyspace: tc.Keyspace,
								},
							},
						},
					},
				},
			},
		},
	}

	vsc := vstreamClientMock{
		vstreamFn: func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
			assert.Equal(t, topodata.TabletType_REPLICA, in.TabletType)
			assert.Contains(t, in.Flags.Cells, "planetscale_operator_default")
			return vstreamSyncClient, nil
		},
	}

	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error) {
		return &vsc, nil
	}
	ps := PlanetScaleSource{
		Database:   "connect-test",
		UseReplica: true,
	}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, vsc.vstreamFnInvokedCount)
	assert.False(t, tma.PingContextFnInvoked)
	assert.False(t, tma.GetVitessTabletsFnInvoked)
}

func TestRead_CanPickRdonlyForShardedKeyspaces(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger(b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "40-80",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	vstreamSyncClient := &vtgateVStreamClientMock{
		vstreamResponses: []*vtgate.VStreamResponse{
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    tc.Shard,
									Gtid:     tc.Position,
									Keyspace: tc.Keyspace,
								},
							},
						},
					},
				},
			},
		},
	}

	vsc := vstreamClientMock{
		vstreamFn: func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
			assert.Equal(t, topodata.TabletType_RDONLY, in.TabletType)
			assert.Contains(t, in.Flags.Cells, "planetscale_operator_default")
			return vstreamSyncClient, nil
		},
	}

	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error) {
		return &vsc, nil
	}
	ps := PlanetScaleSource{
		Database:  "connect-test",
		UseRdonly: true,
	}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, vsc.vstreamFnInvokedCount)
	assert.False(t, tma.PingContextFnInvoked)
	assert.False(t, tma.GetVitessTabletsFnInvoked)
}

func TestDiscover_CanPickRightAirbyteType(t *testing.T) {
	var tests = []struct {
		MysqlType             string
		JSONSchemaType        string
		AirbyteType           string
		TreatTinyIntAsBoolean bool
	}{
		{
			MysqlType:      "int(11)",
			JSONSchemaType: "number",
			AirbyteType:    "integer",
		},
		{
			MysqlType:      "smallint(4)",
			JSONSchemaType: "number",
			AirbyteType:    "integer",
		},
		{
			MysqlType:      "mediumint(8)",
			JSONSchemaType: "number",
			AirbyteType:    "integer",
		},
		{
			MysqlType:             "tinyint",
			JSONSchemaType:        "number",
			AirbyteType:           "integer",
			TreatTinyIntAsBoolean: true,
		},
		{
			MysqlType:             "tinyint(1)",
			JSONSchemaType:        "boolean",
			AirbyteType:           "",
			TreatTinyIntAsBoolean: true,
		},
		{
			MysqlType:             "tinyint(1) unsigned",
			JSONSchemaType:        "boolean",
			AirbyteType:           "",
			TreatTinyIntAsBoolean: true,
		},
		{
			MysqlType:             "tinyint(1)",
			JSONSchemaType:        "number",
			AirbyteType:           "integer",
			TreatTinyIntAsBoolean: false,
		},
		{
			MysqlType:             "tinyint(1) unsigned",
			JSONSchemaType:        "number",
			AirbyteType:           "integer",
			TreatTinyIntAsBoolean: false,
		},
		{
			MysqlType:      "bigint(16)",
			JSONSchemaType: "number",
			AirbyteType:    "integer",
		},
		{
			MysqlType:      "bigint unsigned",
			JSONSchemaType: "number",
			AirbyteType:    "integer",
		},
		{
			MysqlType:      "bigint zerofill",
			JSONSchemaType: "number",
			AirbyteType:    "integer",
		},
		{
			MysqlType:      "datetime",
			JSONSchemaType: "string",
			AirbyteType:    "timestamp_without_timezone",
		},
		{
			MysqlType:      "datetime(6)",
			JSONSchemaType: "string",
			AirbyteType:    "timestamp_without_timezone",
		},
		{
			MysqlType:      "time",
			JSONSchemaType: "string",
			AirbyteType:    "time_without_timezone",
		},
		{
			MysqlType:      "time(6)",
			JSONSchemaType: "string",
			AirbyteType:    "time_without_timezone",
		},
		{
			MysqlType:      "date",
			JSONSchemaType: "string",
			AirbyteType:    "date",
		},
		{
			MysqlType:      "text",
			JSONSchemaType: "string",
			AirbyteType:    "",
		},
		{
			MysqlType:      "varchar(256)",
			JSONSchemaType: "string",
			AirbyteType:    "",
		},
		{
			MysqlType:      "decimal(12,5)",
			JSONSchemaType: "number",
			AirbyteType:    "",
		},
		{
			MysqlType:      "double",
			JSONSchemaType: "number",
			AirbyteType:    "",
		},
		{
			MysqlType:      "float(30)",
			JSONSchemaType: "number",
			AirbyteType:    "",
		},
	}

	for _, typeTest := range tests {

		t.Run(fmt.Sprintf("mysql_type_%v", typeTest.MysqlType), func(t *testing.T) {
			p := getJsonSchemaType(typeTest.MysqlType, typeTest.TreatTinyIntAsBoolean)
			assert.Equal(t, typeTest.AirbyteType, p.AirbyteType)
			assert.Equal(t, typeTest.JSONSchemaType, p.Type)
		})
	}
}

func TestRead_CanPickPrimaryForUnshardedKeyspaces(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger(b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	vstreamSyncClient := &vtgateVStreamClientMock{
		vstreamResponses: []*vtgate.VStreamResponse{
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    tc.Shard,
									Gtid:     tc.Position,
									Keyspace: tc.Keyspace,
								},
							},
						},
					},
				},
			},
		},
	}

	vsc := vstreamClientMock{
		vstreamFn: func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
			assert.Equal(t, topodata.TabletType_PRIMARY, in.TabletType)
			assert.Contains(t, in.Flags.Cells, "planetscale_operator_default")
			return vstreamSyncClient, nil
		},
	}

	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error) {
		return &vsc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, vsc.vstreamFnInvokedCount)
	assert.False(t, tma.PingContextFnInvoked)
	assert.False(t, tma.GetVitessTabletsFnInvoked)
}

func TestRead_CanPickReplicaForUnshardedKeyspaces(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger(b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "MySQL56/0d5afdd6-da80-11ef-844c-26dc1854a614:1-2,e1e896df-dae3-11ef-895b-626e6780cb50:1-2,e50c022a-dade-11ef-8083-d2b0b749d1bb:1-2",
		Keyspace: "connect-test",
	}

	vstreamSyncClient := &vtgateVStreamClientMock{
		vstreamResponses: []*vtgate.VStreamResponse{
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    tc.Shard,
									Gtid:     tc.Position,
									Keyspace: tc.Keyspace,
								},
							},
						},
					},
				},
			},
		},
	}

	vsc := vstreamClientMock{
		vstreamFn: func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
			assert.Equal(t, topodata.TabletType_REPLICA, in.TabletType)
			assert.Contains(t, in.Flags.Cells, "planetscale_operator_default")
			return vstreamSyncClient, nil
		},
	}

	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error) {
		return &vsc, nil
	}
	ps := PlanetScaleSource{
		Database:   "connect-test",
		UseReplica: true,
	}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, vsc.vstreamFnInvokedCount)
	assert.False(t, tma.PingContextFnInvoked)
	assert.False(t, tma.GetVitessTabletsFnInvoked)
}

// CanReturnNewCursorIfNewFound tests returning the same GTid as stop position
func TestRead_IncrementalSync_CanReturnOriginalCursorIfNoNewFound(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger(b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "MySQL56/0d5afdd6-da80-11ef-844c-26dc1854a614:1-2,e1e896df-dae3-11ef-895b-626e6780cb50:1-2,e50c022a-dade-11ef-8083-d2b0b749d1bb:1-2",
		Keyspace: "connect-test",
	}

	vstreamSyncClient := &vtgateVStreamClientMock{
		vstreamResponses: []*vtgate.VStreamResponse{
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    tc.Shard,
									Gtid:     tc.Position,
									Keyspace: tc.Keyspace,
								},
							},
						},
					},
				},
			},
		},
	}

	vsc := vstreamClientMock{
		vstreamFn: func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
			assert.Equal(t, topodata.TabletType_PRIMARY, in.TabletType)
			assert.Contains(t, in.Flags.Cells, "planetscale_operator_default")
			return vstreamSyncClient, nil
		},
	}

	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error) {
		return &vsc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, vsc.vstreamFnInvokedCount)
}

// CanReturnNewCursorIfNewFound tests returning the GTid after the stop position as the start GTid for the next sync
func TestRead_IncrementalSync_CanReturnNewCursorIfNewFound(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger(b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "MySQL56/0d5afdd6-da80-11ef-844c-26dc1854a614:1-2,e1e896df-dae3-11ef-895b-626e6780cb50:1-2,e50c022a-dade-11ef-8083-d2b0b749d1bb:1-2",
		Keyspace: "connect-test",
	}
	newTC := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "MySQL56/0d5afdd6-da80-11ef-844c-26dc1854a614:1-2,e1e896df-dae3-11ef-895b-626e6780cb50:1-3,e50c022a-dade-11ef-8083-d2b0b749d1bb:1-2",
		Keyspace: "connect-test",
	}

	vstreamSyncClient := &vtgateVStreamClientMock{
		vstreamResponses: []*vtgate.VStreamResponse{
			// First sync to get end position
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    newTC.Shard,
									Gtid:     newTC.Position,
									Keyspace: newTC.Keyspace,
								},
							},
						},
					},
				},
			},
			// First recv() of second sync for rows
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    newTC.Shard,
									Gtid:     newTC.Position,
									Keyspace: newTC.Keyspace,
								},
							},
						},
					},
				},
			},
			// Second recv() of second sync for rows
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_ROW,
						RowEvent: &binlogdata.RowEvent{
							TableName: "customers",
							Keyspace:  newTC.Keyspace,
							Shard:     newTC.Shard,
							RowChanges: []*binlogdata.RowChange{
								{
									After: &query.Row{Values: []byte("1,my_name")},
								},
							},
						},
					},
				},
			},
		},
	}

	vsc := vstreamClientMock{
		vstreamFn: func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
			return vstreamSyncClient, nil
		},
	}

	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error) {
		return &vsc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(newTC)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 2, vsc.vstreamFnInvokedCount)
}

// CanStopAtWellKnownCursor tests stopping & flushing records once stop position is passed during an incremental sync
func TestRead_IncrementalSync_CanStopAtWellKnownCursor(t *testing.T) {
	tma := getTestMysqlAccess()
	tal := testAirbyteLogger{}
	ped := PlanetScaleEdgeDatabase{
		Logger: &tal,
		Mysql:  tma,
	}

	numRows := 10
	startVGtidPosition := fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", 0)
	stopVGtidPosition := fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", 8)
	nextSyncVGtidPosition := fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", 9)
	shard := "-"
	keyspace := "connect-test"
	table := "customers"
	database := "connect-test"
	responses := []*vtgate.VStreamResponse{
		{
			Events: []*binlogdata.VEvent{
				{
					Type:     binlogdata.VEventType_BEGIN,
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Type:     binlogdata.VEventType_VGTID,
					Keyspace: keyspace,
					Shard:    shard,
					Vgtid: &binlogdata.VGtid{
						ShardGtids: []*binlogdata.ShardGtid{
							{
								Keyspace: keyspace,
								Shard:    shard,
								Gtid:     fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", 0),
							},
						},
					},
				},
				{
					Type:     binlogdata.VEventType_COMMIT,
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Type: binlogdata.VEventType_FIELD,
					FieldEvent: &binlogdata.FieldEvent{
						TableName: table,
						Fields: []*query.Field{
							{
								Name:         "id",
								Type:         query.Type_INT64,
								Table:        table,
								OrgTable:     table,
								Database:     database,
								ColumnLength: 20,
								Charset:      63,
								ColumnType:   "bigint",
							},
							{
								Name:         "product",
								Type:         query.Type_VARCHAR,
								Table:        table,
								OrgTable:     table,
								Database:     database,
								ColumnLength: 1024,
								Charset:      255,
								ColumnType:   "varchar(256)",
							},
						},
					},
				},
			},
		},
	}
	for i := 1; i < numRows; i++ {
		response := vtgate.VStreamResponse{
			Events: []*binlogdata.VEvent{
				{
					Type:     binlogdata.VEventType_BEGIN,
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Type: binlogdata.VEventType_ROW,
					RowEvent: &binlogdata.RowEvent{
						TableName: table,
						RowChanges: []*binlogdata.RowChange{
							{
								After: &query.Row{
									Values: []byte(fmt.Sprintf("%v,keyboard", i)),
								},
							},
						},
					},
				},
				{
					Type:     binlogdata.VEventType_VGTID,
					Keyspace: keyspace,
					Shard:    shard,
					Vgtid: &binlogdata.VGtid{
						ShardGtids: []*binlogdata.ShardGtid{
							{
								Keyspace: keyspace,
								Shard:    shard,
								Gtid:     fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", i),
							},
						},
					},
				},
				{
					Type:     binlogdata.VEventType_COMMIT,
					Keyspace: keyspace,
					Shard:    shard,
				},
			},
		}
		responses = append(responses, &response)
	}

	getCurrentVGtidClient := &vtgateVStreamClientMock{
		vstreamResponses: []*vtgate.VStreamResponse{
			// First sync to get stop position
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    shard,
									Gtid:     stopVGtidPosition,
									Keyspace: keyspace,
								},
							},
						},
					},
				},
			},
		},
	}

	vstreamSyncClient := &vtgateVStreamClientMock{
		vstreamResponses: responses,
	}

	vsc := vstreamClientMock{
		vstreamFn: func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
			assert.Equal(t, topodata.TabletType_PRIMARY, in.TabletType)
			if in.Vgtid.ShardGtids[0].Gtid == "current" {
				return getCurrentVGtidClient, nil
			}
			return vstreamSyncClient, nil
		},
	}

	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error) {
		return &vsc, nil
	}

	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}

	startCursor := &psdbconnect.TableCursor{
		Shard:    shard,
		Keyspace: keyspace,
		Position: startVGtidPosition,
	}

	expectedCursor := &psdbconnect.TableCursor{
		Shard:    shard,
		Keyspace: keyspace,
		Position: nextSyncVGtidPosition,
	}

	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, startCursor)
	assert.NoError(t, err)
	// Should output next VGtid after stop VGtid as cursor
	esc, err := TableCursorToSerializedCursor(expectedCursor)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 2, vsc.vstreamFnInvokedCount)

	// There were 10 row events total
	assert.Equal(t, 10, len(responses))
	logLines := tal.logMessages[LOGLEVEL_INFO]
	// But only the first 8 (with VGtids <= stop position) will be synced
	assert.Equal(t, fmt.Sprintf("[connect-test:primary:customers shard : -] Finished reading %v records for table [customers]", 8), logLines[len(logLines)-1])
	records := tal.records["connect-test.customers"]
	assert.Equal(t, 8, len(records))
}

// CanLogResults tests synced records from stopping & flushing records once stop position is passed
func TestRead_IncrementalSync_CanLogResults(t *testing.T) {
	tma := getTestMysqlAccess()
	tal := testAirbyteLogger{}
	ped := PlanetScaleEdgeDatabase{
		Logger: &tal,
		Mysql:  tma,
	}

	keyspace := "connect-test"
	shard := "-"
	table := "products"
	startVGtid := "MySQL56/0d5afdd6-da80-11ef-844c-26dc1854a614:1-2,e1e896df-dae3-11ef-895b-626e6780cb50:1-2,e50c022a-dade-11ef-8083-d2b0b749d1bb:1-2"
	nextVGtid := "MySQL56/0d5afdd6-da80-11ef-844c-26dc1854a614:1-2,e1e896df-dae3-11ef-895b-626e6780cb50:1-4,e50c022a-dade-11ef-8083-d2b0b749d1bb:1-2"
	stopVGtid := "MySQL56/0d5afdd6-da80-11ef-844c-26dc1854a614:1-2,e1e896df-dae3-11ef-895b-626e6780cb50:1-3,e50c022a-dade-11ef-8083-d2b0b749d1bb:1-2"

	startCursor := &psdbconnect.TableCursor{
		Shard:    shard,
		Position: startVGtid,
		Keyspace: keyspace,
	}

	vstreamSyncClient := &vtgateVStreamClientMock{
		vstreamResponses: []*vtgate.VStreamResponse{
			// First sync to get stop position
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    shard,
									Gtid:     stopVGtid,
									Keyspace: keyspace,
								},
							},
						},
					},
				},
			},
			// 1st recv() of second sync for rows to get start VGtid
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    shard,
									Gtid:     startVGtid,
									Keyspace: keyspace,
								},
							},
						},
					},
				},
			},
			// 2nd recv() of second sync for rows to get stop VGtid
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    shard,
									Gtid:     stopVGtid,
									Keyspace: keyspace,
								},
							},
						},
					},
				},
			},
			// 3rd recv() for second sync for rows to get fields
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_FIELD,
						FieldEvent: &binlogdata.FieldEvent{
							TableName: table,
							Fields: []*query.Field{
								{
									Name:         "pid",
									Type:         query.Type_INT64,
									Table:        table,
									OrgTable:     table,
									Database:     keyspace,
									ColumnLength: 20,
									Charset:      63,
									ColumnType:   "bigint",
								},
								{
									Name:         "description",
									Type:         query.Type_VARCHAR,
									Table:        table,
									OrgTable:     table,
									Database:     keyspace,
									ColumnLength: 1024,
									Charset:      255,
									ColumnType:   "varchar(256)",
								},
							},
						},
					},
				},
			},
			// 4th recv() of second sync for rows to get records
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_ROW,
						RowEvent: &binlogdata.RowEvent{
							TableName: table,
							Keyspace:  keyspace,
							Shard:     shard,
							RowChanges: []*binlogdata.RowChange{
								{
									After: &query.Row{
										Lengths: []int64{1, 8},
										Values:  []byte("1keyboard"),
									},
								},
								{
									After: &query.Row{
										Lengths: []int64{1, 7},
										Values:  []byte("2monitor"),
									},
								},
							},
						},
					},
				},
			},
			// 5th recv() of second sync for rows to advance GTid past stop position
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    shard,
									Gtid:     nextVGtid,
									Keyspace: keyspace,
								},
							},
						},
					},
				},
			},
			// Will not reach this event since stop position passed
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_ROW,
						RowEvent: &binlogdata.RowEvent{
							TableName: table,
							Keyspace:  keyspace,
							Shard:     shard,
							RowChanges: []*binlogdata.RowChange{
								{
									After: &query.Row{
										Lengths: []int64{1, 8},
										Values:  []byte("1keyboard"),
									},
								},
								{
									After: &query.Row{
										Lengths: []int64{1, 7},
										Values:  []byte("2monitor"),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	vsc := vstreamClientMock{
		vstreamFn: func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
			assert.Equal(t, topodata.TabletType_PRIMARY, in.TabletType)
			return vstreamSyncClient, nil
		},
	}

	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error) {
		return &vsc, nil
	}

	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "products",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, startCursor)
	assert.NoError(t, err)
	assert.NotNil(t, sc)
	assert.Equal(t, 2, len(tal.records["connect-test.products"]))
	records := tal.records["connect-test.products"]
	keyboardFound := false
	monitorFound := false
	for _, r := range records {
		id, err := r["pid"].(sqltypes.Value).ToInt64()
		assert.NoError(t, err)
		if id == 1 {
			assert.False(t, keyboardFound, "should not find keyboard twice")
			keyboardFound = true
			assert.Equal(t, "keyboard", r["description"].(sqltypes.Value).ToString())
		}

		if id == 2 {
			assert.False(t, monitorFound, "should not find monitor twice")
			monitorFound = true
			assert.Equal(t, "monitor", r["description"].(sqltypes.Value).ToString())
		}
	}
	assert.True(t, keyboardFound)
	assert.True(t, monitorFound)
}

// CanStopIfNoRows tests stopping even if no rows are found during an incremental sync
func TestRead_IncrementalSync_CanStopIfNoRows(t *testing.T) {
	tma := getTestMysqlAccess()
	tal := testAirbyteLogger{}
	ped := PlanetScaleEdgeDatabase{
		Logger: &tal,
		Mysql:  tma,
	}

	keyspace := "connect-test"
	shard := "-"
	table := "products"
	startVGtid := "MySQL56/0d5afdd6-da80-11ef-844c-26dc1854a614:1-2,e1e896df-dae3-11ef-895b-626e6780cb50:1-2,e50c022a-dade-11ef-8083-d2b0b749d1bb:1-2"
	nextVGtid := "MySQL56/0d5afdd6-da80-11ef-844c-26dc1854a614:1-2,e1e896df-dae3-11ef-895b-626e6780cb50:1-4,e50c022a-dade-11ef-8083-d2b0b749d1bb:1-2"
	stopVGtid := "MySQL56/0d5afdd6-da80-11ef-844c-26dc1854a614:1-2,e1e896df-dae3-11ef-895b-626e6780cb50:1-3,e50c022a-dade-11ef-8083-d2b0b749d1bb:1-2"

	startCursor := &psdbconnect.TableCursor{
		Shard:    shard,
		Position: startVGtid,
		Keyspace: keyspace,
	}

	vstreamSyncClient := &vtgateVStreamClientMock{
		vstreamResponses: []*vtgate.VStreamResponse{
			// First sync to get stop position
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    shard,
									Gtid:     stopVGtid,
									Keyspace: keyspace,
								},
							},
						},
					},
				},
			},
			// 1st recv() of second sync for rows to get start VGtid
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    shard,
									Gtid:     startVGtid,
									Keyspace: keyspace,
								},
							},
						},
					},
				},
			},
			// 2nd recv() of second sync for rows to get stop VGtid
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    shard,
									Gtid:     stopVGtid,
									Keyspace: keyspace,
								},
							},
						},
					},
				},
			},
			// 3rd recv() of second sync for rows to advance GTid past stop position
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    shard,
									Gtid:     nextVGtid,
									Keyspace: keyspace,
								},
							},
						},
					},
				},
			},
			// Will not reach this event since stop position passed
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_ROW,
						RowEvent: &binlogdata.RowEvent{
							TableName: table,
							Keyspace:  keyspace,
							Shard:     shard,
							RowChanges: []*binlogdata.RowChange{
								{
									After: &query.Row{
										Lengths: []int64{1, 8},
										Values:  []byte("1keyboard"),
									},
								},
								{
									After: &query.Row{
										Lengths: []int64{1, 7},
										Values:  []byte("2monitor"),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	vsc := vstreamClientMock{
		vstreamFn: func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
			assert.Equal(t, topodata.TabletType_PRIMARY, in.TabletType)
			return vstreamSyncClient, nil
		},
	}

	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error) {
		return &vsc, nil
	}

	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "products",
			Namespace: "connect-test",
		},
	}
	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, startCursor)
	assert.NoError(t, err)
	assert.NotNil(t, sc)
	assert.Equal(t, 0, len(tal.records["connect-test.products"]))
}

func getTestMysqlAccess() *mysqlAccessMock {
	tma := mysqlAccessMock{
		PingContextFn: func(ctx context.Context, source PlanetScaleSource) error {
			return nil
		},
		GetVitessTabletsFn: func(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error) {
			return []VitessTablet{
				{
					Cell:       "test_cell_primary",
					Keyspace:   "connect-test",
					TabletType: TabletTypeToString(psdbconnect.TabletType_primary),
					State:      "SERVING",
				},
				{
					Cell:       "test_cell_replica",
					Keyspace:   "connect-test",
					TabletType: TabletTypeToString(psdbconnect.TabletType_replica),
					State:      "SERVING",
				},
			}, nil
		},
	}
	return &tma
}

/*
*
CanSyncPastStopPosition tests the following situation:
1. Full sync (no start cursor)
2. Connector reaches out to fetch the current VGTID position as the "stop VGTID"
3. We sync from the beginning (no start cursor) to the current VGTID position "current VGTID position"
  - "current VGTID position" could already be ahead of the prior step's "stop VGTID" because of heartbeats, write frequency, etc.

4. "current VGTID position" is after "stop VGTID"
5. Since the "current VGTID position" is already after the "stop VGTID", we can stop the sync and flush records
6. We return "next VGTID position" (the first VGTID position that is after the "stop VGTID") as the "start cursor" for the next sync
*
*/
func TestRead_FullSync_CanStopSyncPastStopPosition(t *testing.T) {
	tma := getTestMysqlAccess()
	tal := testAirbyteLogger{}
	ped := PlanetScaleEdgeDatabase{
		Logger: &tal,
		Mysql:  tma,
	}

	startCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "",
	}
	stopVGtidPosition := fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", 8)
	nextSyncVGtidPosition := fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", 9)

	numRows := 10
	shard := "-"
	keyspace := "connect-test"
	table := "customers"
	database := "connect-test"

	responses := []*vtgate.VStreamResponse{
		{
			Events: []*binlogdata.VEvent{
				{
					Type:     binlogdata.VEventType_BEGIN,
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Type:     binlogdata.VEventType_VGTID,
					Keyspace: keyspace,
					Shard:    shard,
					Vgtid: &binlogdata.VGtid{
						ShardGtids: []*binlogdata.ShardGtid{
							{
								Keyspace: keyspace,
								Shard:    shard,
								Gtid:     fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", 9),
							},
						},
					},
				},
				{
					Type:     binlogdata.VEventType_COMMIT,
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Type: binlogdata.VEventType_FIELD,
					FieldEvent: &binlogdata.FieldEvent{
						TableName: table,
						Fields: []*query.Field{
							{
								Name:         "id",
								Type:         query.Type_INT64,
								Table:        table,
								OrgTable:     table,
								Database:     database,
								ColumnLength: 20,
								Charset:      63,
								ColumnType:   "bigint",
							},
							{
								Name:         "product",
								Type:         query.Type_VARCHAR,
								Table:        table,
								OrgTable:     table,
								Database:     database,
								ColumnLength: 1024,
								Charset:      255,
								ColumnType:   "varchar(256)",
							},
						},
					},
				},
			},
		},
	}
	rowResponse := vtgate.VStreamResponse{
		Events: []*binlogdata.VEvent{
			{
				Type:     binlogdata.VEventType_BEGIN,
				Keyspace: keyspace,
				Shard:    shard,
			},
		},
	}
	for i := 0; i < numRows; i++ {
		event := binlogdata.VEvent{
			Type: binlogdata.VEventType_ROW,
			RowEvent: &binlogdata.RowEvent{
				TableName: table,
				RowChanges: []*binlogdata.RowChange{
					{
						After: &query.Row{
							Values: []byte(fmt.Sprintf("%v,keyboard", 9)),
						},
					},
				},
			},
		}
		rowResponse.Events = append(rowResponse.Events, &event)
	}

	responses = append(responses, &rowResponse)

	responses = append(responses, &vtgate.VStreamResponse{
		Events: []*binlogdata.VEvent{
			{
				Type: binlogdata.VEventType_COPY_COMPLETED,
			},
		},
	})

	responses = append(responses, &vtgate.VStreamResponse{
		Events: []*binlogdata.VEvent{
			{
				Type:     binlogdata.VEventType_VGTID,
				Keyspace: keyspace,
				Shard:    shard,
				Vgtid: &binlogdata.VGtid{
					ShardGtids: []*binlogdata.ShardGtid{
						{
							Keyspace: keyspace,
							Shard:    shard,
							Gtid:     "MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-10",
						},
					},
				},
			},
		},
	})

	getCurrentVGtidClient := &vtgateVStreamClientMock{
		vstreamResponses: []*vtgate.VStreamResponse{
			// First sync to get stop position
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    shard,
									Gtid:     stopVGtidPosition,
									Keyspace: keyspace,
								},
							},
						},
					},
				},
			},
		},
	}

	vstreamSyncClient := &vtgateVStreamClientMock{
		vstreamResponses: responses,
	}

	vsc := vstreamClientMock{
		vstreamFn: func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
			assert.Equal(t, topodata.TabletType_PRIMARY, in.TabletType)
			if in.Vgtid.ShardGtids[0].Gtid == "current" {
				return getCurrentVGtidClient, nil
			}
			return vstreamSyncClient, nil
		},
	}

	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error) {
		return &vsc, nil
	}

	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}

	nextSyncStartCursor, err := ped.Read(context.Background(), os.Stdout, ps, cs, startCursor)
	assert.NoError(t, err)
	// Next sync will start at the VGTID after the end of the current sync
	esc, err := TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: nextSyncVGtidPosition,
	})
	assert.NoError(t, err)
	assert.Equal(t, esc, nextSyncStartCursor)
	assert.Equal(t, 2, vsc.vstreamFnInvokedCount)

	logLines := tal.logMessages[LOGLEVEL_INFO]
	assert.Equal(t, fmt.Sprintf("[connect-test:primary:customers shard : -] Finished reading %v records for table [customers]", 10), logLines[len(logLines)-1])
	records := tal.records["connect-test.customers"]
	assert.Equal(t, 10, len(records))
}

/*
*
CanSyncPastStopPosition tests the following situation:
1. Full sync (no start cursor)
2. Connector reaches out to fetch the current VGTID position as the "stop VGTID"
3. We sync from the beginning (no start cursor) to the current VGTID position "current VGTID position"
4. "current VGTID position" is EQUAL TO "stop VGTID"
5. Since the "current VGTID position" is EQUAL TO "stop VGTID", we can stop the sync and flush records
6. We return "next VGTID position" (the first VGTID position that is after the "stop VGTID") as the "start cursor" for the next sync
*
*/
func TestRead_FullSync_CanStopSyncEqualToStopPosition(t *testing.T) {
	tma := getTestMysqlAccess()
	tal := testAirbyteLogger{}
	ped := PlanetScaleEdgeDatabase{
		Logger: &tal,
		Mysql:  tma,
	}

	startCursor := &psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: "",
	}
	stopVGtidPosition := fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", 8)
	nextSyncVGtidPosition := fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", 9)

	numRows := 10
	shard := "-"
	keyspace := "connect-test"
	table := "customers"
	database := "connect-test"

	responses := []*vtgate.VStreamResponse{
		{
			Events: []*binlogdata.VEvent{
				{
					Type:     binlogdata.VEventType_BEGIN,
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Type:     binlogdata.VEventType_VGTID,
					Keyspace: keyspace,
					Shard:    shard,
					Vgtid: &binlogdata.VGtid{
						ShardGtids: []*binlogdata.ShardGtid{
							{
								Keyspace: keyspace,
								Shard:    shard,
								Gtid:     stopVGtidPosition,
							},
						},
					},
				},
				{
					Type:     binlogdata.VEventType_COMMIT,
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Type: binlogdata.VEventType_FIELD,
					FieldEvent: &binlogdata.FieldEvent{
						TableName: table,
						Fields: []*query.Field{
							{
								Name:         "id",
								Type:         query.Type_INT64,
								Table:        table,
								OrgTable:     table,
								Database:     database,
								ColumnLength: 20,
								Charset:      63,
								ColumnType:   "bigint",
							},
							{
								Name:         "product",
								Type:         query.Type_VARCHAR,
								Table:        table,
								OrgTable:     table,
								Database:     database,
								ColumnLength: 1024,
								Charset:      255,
								ColumnType:   "varchar(256)",
							},
						},
					},
				},
			},
		},
	}
	rowResponse := vtgate.VStreamResponse{
		Events: []*binlogdata.VEvent{
			{
				Type:     binlogdata.VEventType_BEGIN,
				Keyspace: keyspace,
				Shard:    shard,
			},
		},
	}
	for i := 0; i < numRows; i++ {
		event := binlogdata.VEvent{
			Type: binlogdata.VEventType_ROW,
			RowEvent: &binlogdata.RowEvent{
				TableName: table,
				RowChanges: []*binlogdata.RowChange{
					{
						After: &query.Row{
							Values: []byte(fmt.Sprintf("%v,keyboard", 9)),
						},
					},
				},
			},
		}
		rowResponse.Events = append(rowResponse.Events, &event)
	}

	responses = append(responses, &rowResponse)

	responses = append(responses, &vtgate.VStreamResponse{
		Events: []*binlogdata.VEvent{
			{
				Type: binlogdata.VEventType_COPY_COMPLETED,
			},
		},
	})

	responses = append(responses, &vtgate.VStreamResponse{
		Events: []*binlogdata.VEvent{
			{
				Type:     binlogdata.VEventType_VGTID,
				Keyspace: keyspace,
				Shard:    shard,
				Vgtid: &binlogdata.VGtid{
					ShardGtids: []*binlogdata.ShardGtid{
						{
							Keyspace: keyspace,
							Shard:    shard,
							Gtid:     nextSyncVGtidPosition,
						},
					},
				},
			},
		},
	})

	getCurrentVGtidClient := &vtgateVStreamClientMock{
		vstreamResponses: []*vtgate.VStreamResponse{
			// First sync to get stop position
			{
				Events: []*binlogdata.VEvent{
					{
						Type: binlogdata.VEventType_VGTID,
						Vgtid: &binlogdata.VGtid{
							ShardGtids: []*binlogdata.ShardGtid{
								{
									Shard:    shard,
									Gtid:     stopVGtidPosition,
									Keyspace: keyspace,
								},
							},
						},
					},
				},
			},
		},
	}

	vstreamSyncClient := &vtgateVStreamClientMock{
		vstreamResponses: responses,
	}

	vsc := vstreamClientMock{
		vstreamFn: func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
			assert.Equal(t, topodata.TabletType_PRIMARY, in.TabletType)
			if in.Vgtid.ShardGtids[0].Gtid == "current" {
				return getCurrentVGtidClient, nil
			}
			return vstreamSyncClient, nil
		},
	}

	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error) {
		return &vsc, nil
	}

	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := ConfiguredStream{
		Stream: Stream{
			Name:      "customers",
			Namespace: "connect-test",
		},
	}

	nextSyncStartCursor, err := ped.Read(context.Background(), os.Stdout, ps, cs, startCursor)
	assert.NoError(t, err)
	// Next sync will start at the VGTID after the end of the current sync
	esc, err := TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "connect-test",
		Position: nextSyncVGtidPosition,
	})
	assert.NoError(t, err)
	assert.Equal(t, esc, nextSyncStartCursor)
	assert.Equal(t, 2, vsc.vstreamFnInvokedCount)

	logLines := tal.logMessages[LOGLEVEL_INFO]
	assert.Equal(t, fmt.Sprintf("[connect-test:primary:customers shard : -] Finished reading %v records for table [customers]", 10), logLines[len(logLines)-1])
	records := tal.records["connect-test.customers"]
	assert.Equal(t, 10, len(records))
}
