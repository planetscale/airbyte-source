package main

import (
	"context"
	"github.com/planetscale/airbyte-source/cmd/airbyte-source"
	"os"
	"os/signal"
)

var (
	version string
	commit  string
	date    string
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	airbyte_source.Execute(ctx, version, commit, date)
}
