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

func TestRead_CanReturnOriginalCursorIfNoNewFound(t *testing.T) {
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
}

func TestRead_CanReturnNewCursorIfNewFound(t *testing.T) {
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

// func TestRead_CanStopAtWellKnownCursor(t *testing.T) {
// 	tma := getTestMysqlAccess()
// 	tal := testAirbyteLogger{}
// 	ped := PlanetScaleEdgeDatabase{
// 		Logger: &tal,
// 		Mysql:  tma,
// 	}

// 	numResponses := 10
// 	numRows := 10
// 	stopVGtidPosition := fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", 8)
// 	nextSyncVGtidPosition := fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", 9)
// 	// when the client tries to get the "current" vgtid,
// 	// we return the ante-penultimate element of the array.
// 	currentVGtidPosition := (numResponses * 3) - 4 // position 26, GTID: 1-8
// 	// this is the next vgtid that should stop the sync session.
// 	nextVGtidPosition := currentVGtidPosition + 1 // position 27, GTID: 1-9
// 	responses := make([]*psdbconnect.SyncResponse, 0, numResponses)
// 	rowEvents := make([]*binlogdata.VEvent, 0, numRows)
// 	for i := 0; i < numRows; i++ {
// 		event := &binlogdata.RowEvent{

// 		}
// 	}

// 	for i := 0; i < numResponses; i++ { // 10
// 		// this simulates multiple events being returned, for the same vgtid, from vstream
// 		for x := 0; x < 3; x++ {
// 			var result []*query.QueryResult
// 			if x == 2 {
// 				result = []*query.QueryResult{
// 					sqltypes.ResultToProto3(sqltypes.MakeTestResult(sqltypes.MakeTestFields(
// 						"pid|description",
// 						"int64|varbinary"),
// 						fmt.Sprintf("%v|keyboard", i+1),
// 						fmt.Sprintf("%v|monitor", i+2),
// 					)),
// 				}
// 			}

// 			vgtid := fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", i)
// 			responses = append(responses, &psdbconnect.SyncResponse{
// 				Cursor: &psdbconnect.TableCursor{
// 					Shard:    "-",
// 					Keyspace: "connect-test",
// 					Position: vgtid,
// 				},
// 				Result: result,
// 			})
// 		}
// 	}

// 	getCurrentVGtidClient := &vtgateVStreamClientMock{
// 		vstreamResponses: []*vtgate.VStreamResponse{
// 			// First sync to get end position
// 			{
// 				Events: []*binlogdata.VEvent{
// 					{
// 						Type: binlogdata.VEventType_VGTID,
// 						Vgtid: &binlogdata.VGtid{
// 							ShardGtids: []*binlogdata.ShardGtid{
// 								{
// 									Shard:    responses[currentVGtidPosition].Cursor.Shard,
// 									Gtid:     responses[currentVGtidPosition].Cursor.Position,
// 									Keyspace: responses[currentVGtidPosition].Cursor.Keyspace,
// 								},
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 	}

// 	vstreamSyncClient := &vtgateVStreamClientMock{
// 		vstreamResponses: []*vtgate.VStreamResponse{
// 			// First sync to get end position
// 			{
// 				Events: []*binlogdata.VEvent{
// 					{
// 						Type: binlogdata.VEventType_VGTID,
// 						Vgtid: &binlogdata.VGtid{
// 							ShardGtids: []*binlogdata.ShardGtid{
// 								{
// 									Shard:    newTC.Shard,
// 									Gtid:     newTC.Position,
// 									Keyspace: newTC.Keyspace,
// 								},
// 							},
// 						},
// 					},
// 				},
// 			},
// 			// First recv() of second sync for rows
// 			{
// 				Events: []*binlogdata.VEvent{
// 					{
// 						Type: binlogdata.VEventType_VGTID,
// 						Vgtid: &binlogdata.VGtid{
// 							ShardGtids: []*binlogdata.ShardGtid{
// 								{
// 									Shard:    newTC.Shard,
// 									Gtid:     newTC.Position,
// 									Keyspace: newTC.Keyspace,
// 								},
// 							},
// 						},
// 					},
// 				},
// 			},
// 			// Second recv() of second sync for rows
// 			{
// 				Events: []*binlogdata.VEvent{
// 					{
// 						Type: binlogdata.VEventType_ROW,
// 						RowEvent: &binlogdata.RowEvent{
// 							TableName: "customers",
// 							Keyspace:  newTC.Keyspace,
// 							Shard:     newTC.Shard,
// 							RowChanges: []*binlogdata.RowChange{
// 								{
// 									After: &query.Row{Values: []byte("1,my_name")},
// 								},
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 	}

// 	vsc := vstreamClientMock{
// 		vstreamFn: func(ctx context.Context, in *vtgate.VStreamRequest, opts ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
// 			assert.Equal(t, topodata.TabletType_PRIMARY, in.TabletType)
// 			if in.Vgtid.ShardGtids[0].Gtid == "current" {
// 				return getCurrentVGtidClient, nil
// 			}
// 			return vstreamSyncClient, nil
// 		},
// 	}

// 	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error) {
// 		return &vsc, nil
// 	}

// 	ps := PlanetScaleSource{
// 		Database: "connect-test",
// 	}
// 	cs := ConfiguredStream{
// 		Stream: Stream{
// 			Name:      "customers",
// 			Namespace: "connect-test",
// 		},
// 	}

// 	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, responses[0].Cursor)
// 	assert.NoError(t, err)
// 	// sync should start at the first vgtid
// 	esc, err := TableCursorToSerializedCursor(responses[nextVGtidPosition].Cursor)
// 	assert.NoError(t, err)
// 	assert.Equal(t, esc, sc)
// 	assert.Equal(t, 2, cc.syncFnInvokedCount)

// 	logLines := tal.logMessages[LOGLEVEL_INFO]
// 	assert.Equal(t, fmt.Sprintf("[connect-test:primary:customers shard : -] Finished reading %v records for table [customers]", nextVGtidPosition/3*2), logLines[len(logLines)-1])
// 	records := tal.records["connect-test.customers"]
// 	assert.Equal(t, 2*(nextVGtidPosition/3), len(records))
// }

// func TestRead_CanLogResults(t *testing.T) {
// 	tma := getTestMysqlAccess()
// 	tal := testAirbyteLogger{}
// 	ped := PlanetScaleEdgeDatabase{
// 		Logger: &tal,
// 		Mysql:  tma,
// 	}
// 	tc := &psdbconnect.TableCursor{
// 		Shard:    "-",
// 		Position: "MySQL56/0d5afdd6-da80-11ef-844c-26dc1854a614:1-2,e1e896df-dae3-11ef-895b-626e6780cb50:1-2,e50c022a-dade-11ef-8083-d2b0b749d1bb:1-2",
// 		Keyspace: "connect-test",
// 	}
// 	newTC := &psdbconnect.TableCursor{
// 		Shard:    "-",
// 		Position: "MySQL56/0d5afdd6-da80-11ef-844c-26dc1854a614:1-2,e1e896df-dae3-11ef-895b-626e6780cb50:1-3,e50c022a-dade-11ef-8083-d2b0b749d1bb:1-2",
// 		Keyspace: "connect-test",
// 	}

// 	result := []*query.QueryResult{
// 		sqltypes.ResultToProto3(sqltypes.MakeTestResult(sqltypes.MakeTestFields(
// 			"pid|description",
// 			"int64|varbinary"),
// 			"1|keyboard",
// 			"2|monitor",
// 		)),
// 	}

// 	syncClient := &connectSyncClientMock{
// 		syncResponses: []*psdbconnect.SyncResponse{
// 			{Cursor: newTC, Result: result},
// 			{Cursor: newTC, Result: result},
// 		},
// 	}

// 	cc := clientConnectionMock{
// 		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
// 			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
// 			return syncClient, nil
// 		},
// 	}
// 	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
// 		return &cc, nil
// 	}
// 	ps := PlanetScaleSource{
// 		Database: "connect-test",
// 	}
// 	cs := ConfiguredStream{
// 		Stream: Stream{
// 			Name:      "products",
// 			Namespace: "connect-test",
// 		},
// 	}
// 	sc, err := ped.Read(context.Background(), os.Stdout, ps, cs, tc)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, sc)
// 	assert.Equal(t, 2, len(tal.records["connect-test.products"]))
// 	records := tal.records["connect-test.products"]
// 	keyboardFound := false
// 	monitorFound := false
// 	for _, r := range records {
// 		id, err := r["pid"].(sqltypes.Value).ToInt64()
// 		assert.NoError(t, err)
// 		if id == 1 {
// 			assert.False(t, keyboardFound, "should not find keyboard twice")
// 			keyboardFound = true
// 			assert.Equal(t, "keyboard", r["description"].(sqltypes.Value).ToString())
// 		}

// 		if id == 2 {
// 			assert.False(t, monitorFound, "should not find monitor twice")
// 			monitorFound = true
// 			assert.Equal(t, "monitor", r["description"].(sqltypes.Value).ToString())
// 		}
// 	}
// 	assert.True(t, keyboardFound)
// 	assert.True(t, monitorFound)
// }

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

// /*
// *
// CanSyncPastStopPosition tests the following situation:
// 1. Full sync (no start cursor)
// 2. Connector reaches out to fetch the current VGTID position as the "stop VGTID"
// 3. We sync from the beginning (no start cursor) to the current VGTID position "current VGTID position"
//   - "current VGTID position" could already be ahead of the prior step's "stop VGTID" because of heartbeats, write frequency, etc.

// 4. "current VGTID position" is after "stop VGTID"
// 5. Since the "current VGTID position" is already after the "stop VGTID", we can stop the sync and flush records
// 6. We return "next VGTID position" (the first VGTID position that is after the "stop VGTID") as the "start cursor" for the next sync
// *
// */
// func TestRead_CanStopSyncPastStopPosition(t *testing.T) {
// 	tma := getTestMysqlAccess()
// 	tal := testAirbyteLogger{}
// 	ped := PlanetScaleEdgeDatabase{
// 		Logger: &tal,
// 		Mysql:  tma,
// 	}

// 	stopVGTIDNumber := 7
// 	currentVGTIDNumber := 8
// 	nextVGTIDNumber := currentVGTIDNumber
// 	stopVGtidPosition := fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", stopVGTIDNumber)
// 	currentVGTIDPosition := fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", currentVGTIDNumber)
// 	nextVGtidPosition := fmt.Sprintf("MySQL56/e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", nextVGTIDNumber)
// 	startCursor := &psdbconnect.TableCursor{
// 		Shard:    "-",
// 		Keyspace: "connect-test",
// 		Position: "",
// 	}

// 	results := []*query.QueryResult{
// 		sqltypes.ResultToProto3(sqltypes.MakeTestResult(sqltypes.MakeTestFields(
// 			"pid|description",
// 			"int64|varbinary"),
// 			fmt.Sprintf("%v|keyboard", 1),
// 			fmt.Sprintf("%v|monitor", 2),
// 			fmt.Sprintf("%v|keyboard", 3),
// 			fmt.Sprintf("%v|monitor", 4),
// 			fmt.Sprintf("%v|keyboard", 5),
// 			fmt.Sprintf("%v|monitor", 6),
// 			fmt.Sprintf("%v|keyboard", 7),
// 			fmt.Sprintf("%v|monitor", 8),
// 			fmt.Sprintf("%v|keyboard", 9),
// 			fmt.Sprintf("%v|monitor", 10),
// 		)),
// 	}

// 	responses := []*psdbconnect.SyncResponse{
// 		// A heartbeat response that can occur first, with the same VGTID as a response with rows
// 		{
// 			Cursor: &psdbconnect.TableCursor{
// 				Shard:    "-",
// 				Keyspace: "connect-test",
// 				Position: currentVGTIDPosition,
// 			},
// 		},
// 		// A response with rows
// 		{
// 			Cursor: &psdbconnect.TableCursor{
// 				Shard:    "-",
// 				Keyspace: "connect-test",
// 				Position: currentVGTIDPosition,
// 			},
// 			Result: results,
// 		},
// 	}

// 	syncClient := &connectSyncClientMock{
// 		syncResponses: responses,
// 	}

// 	getStopVGtidClient := &connectSyncClientMock{
// 		syncResponses: []*psdbconnect.SyncResponse{
// 			{
// 				Cursor: &psdbconnect.TableCursor{
// 					Shard:    "-",
// 					Keyspace: "connect-test",
// 					Position: stopVGtidPosition,
// 				},
// 			},
// 		},
// 	}

// 	cc := clientConnectionMock{
// 		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
// 			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
// 			if in.Cursor.Position == "current" {
// 				// Returned while fetching the "stop VGTID cursor"
// 				return getStopVGtidClient, nil
// 			}

// 			// Returned during sync of records, after "stop VGTID cursor" is fetched
// 			return syncClient, nil
// 		},
// 	}

// 	ped.vtgateClientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
// 		return &cc, nil
// 	}
// 	ps := PlanetScaleSource{
// 		Database: "connect-test",
// 	}
// 	cs := ConfiguredStream{
// 		Stream: Stream{
// 			Name:      "customers",
// 			Namespace: "connect-test",
// 		},
// 	}

// 	nextSyncStartCursor, err := ped.Read(context.Background(), os.Stdout, ps, cs, startCursor)
// 	assert.NoError(t, err)
// 	// Next sync will start at the VGTID after the end of the current sync
// 	esc, err := TableCursorToSerializedCursor(&psdbconnect.TableCursor{
// 		Shard:    "-",
// 		Keyspace: "connect-test",
// 		Position: nextVGtidPosition,
// 	})
// 	assert.NoError(t, err)
// 	assert.Equal(t, esc, nextSyncStartCursor)
// 	assert.Equal(t, 2, cc.syncFnInvokedCount)

// 	logLines := tal.logMessages[LOGLEVEL_INFO]
// 	assert.Equal(t, fmt.Sprintf("[connect-test:primary:customers shard : -] Finished reading %v records for table [customers]", 10), logLines[len(logLines)-1])
// 	records := tal.records["connect-test.customers"]
// 	assert.Equal(t, 10, len(records))
// }
