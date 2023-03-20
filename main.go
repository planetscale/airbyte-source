package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/planetscale/airbyte-source/cmd/airbyte-source"
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
