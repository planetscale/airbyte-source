package cmd

import (
	"fmt"
	"strings"
)

type Stream struct {
	Name               string       `json:"name"`
	Schema             StreamSchema `json:"json_schema"`
	SupportedSyncModes []string     `json:"supported_sync_modes"`
	Namespace          string       `json:"namespace"`
}

func (s *Stream) GetSelectQuery() string {
	var columns []string
	for name, _ := range s.Schema.Properties {
		columns = append(columns, name)
	}
	return fmt.Sprintf("Select %v from %v LIMIT 10", strings.Join(columns, ","), s.Name)
}

//func (table Stream) Read(psc PlanetScaleConnection, state string) {
//	db, err := sql.Open("mysql", psc.DSN())
//	if err != nil {
//		log.Printf("Unable to open connection to read stream : %v", err)
//		return
//	}
//	cursor, err := db.Query(table.GetSelectQuery())
//	if err != nil {
//		log.Printf("Unable to query rows from table  : %v, query %v failed with %v : ", table.Name, table.GetSelectQuery(), err)
//		return
//	}
//
//	cols, _ := cursor.Columns()
//	columns := make([]string, len(table.Schema.Properties))
//	columnPointers := make([]interface{}, len(table.Schema.Properties))
//	for i, _ := range columns {
//		columnPointers[i] = &columns[i]
//	}
//	for cursor.Next() {
//		err := cursor.Scan(columnPointers...)
//		if err != nil {
//			// handle err
//			log.Fatal(err)
//		}
//		// Create our map, and retrieve the value for each column from the pointers slice,
//		// storing it in the map with the name of the column as the key.
//		m := make(map[string]string)
//		for i, colName := range cols {
//			val := columnPointers[i].(*string)
//			m[colName] = *val
//		}
//
//		printRecord(table, m)
//		printState(map[string]string{
//			"vgtid": "{shard_gtids:{keyspace:\"keyspace\" shard:\"-\" gtid:\"MySQL56/32f1f690-8465-11ec-a0c9-46f19e9f0fcb:1-59\"}}",
//		})
//	}
//}
//
//func printRecord(table Stream, record map[string]string) error {
//	now := time.Now()
//	amsg := AirbyteMessage{
//		Type: RECORD,
//		Record: &AirbyteRecord{
//			Stream:    table.Name,
//			Data:      record,
//			EmittedAt: now.UnixMilli(),
//		},
//	}
//
//	msg, _ := json.Marshal(amsg)
//	fmt.Println(string(msg))
//	return nil
//}
//
//func printState(state map[string]string) {
//	amsg := AirbyteMessage{
//		Type:  STATE,
//		State: &AirbyteState{state},
//	}
//
//	msg, _ := json.Marshal(amsg)
//	fmt.Println(string(msg))
//}
