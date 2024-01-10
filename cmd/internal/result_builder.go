package internal

import (
	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/connect-sdk/lib"
	"vitess.io/vitess/go/sqltypes"
)

type ResultBuilder struct {
	keyspace       string
	table          string
	logger         AirbyteLogger
	HandleOnCursor func(tc *psdbconnect.TableCursor) error
}

func NewResultBuilder(logger AirbyteLogger) lib.ResultBuilder {
	return &ResultBuilder{
		logger: logger,
	}
}

func (rb *ResultBuilder) OnResult(result *sqltypes.Result, operation lib.Operation) error {
	data := QueryResultToRecords(result)
	for _, record := range data {
		rb.logger.Record(rb.keyspace, rb.table, record)
	}
	return nil
}

func (ResultBuilder) OnUpdate(row *lib.UpdatedRow) error {
	return nil
}

func (rb *ResultBuilder) OnCursor(cursor *psdbconnect.TableCursor) error {
	if rb.HandleOnCursor == nil {
		return errors.New("Unhandled onCursor event")
	}

	return rb.HandleOnCursor(cursor)
}

func (rb *ResultBuilder) SetKeyspace(keyspace string) {
	rb.keyspace = keyspace
}

func (rb *ResultBuilder) SetTable(table string) {
	rb.table = table
}
