package internal

import "github.com/planetscale/airbyte-source/lib"

type schemaBuilder struct {
	treatTinyIntAsBoolean bool
	catalog               *Catalog
}

func NewSchemaBuilder(treatTinyIntAsBoolean bool) lib.SchemaBuilder {
	return &schemaBuilder{
		treatTinyIntAsBoolean: treatTinyIntAsBoolean,
	}
}

func (sb *schemaBuilder) OnKeyspace(keyspaceName string) {
	if sb.catalog == nil {
		sb.catalog = &Catalog{}
	}
	panic("implement me")
}

func (schemaBuilder) OnTable(keyspaceName, tableName string) {
	//TODO implement me
	panic("implement me")
}

func (schemaBuilder) OnColumns(keyspaceName, tableName string, columns []lib.MysqlColumn) {
	//TODO implement me
	panic("implement me")
}
