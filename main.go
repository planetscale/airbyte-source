package main

import (
	"context"
	"os"
	"os/signal"

	airbytesource "github.com/planetscale/airbyte-source/cmd/airbyte-source"
)

var (
	version string
	commit  string
	date    string
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	airbytesource.Execute(ctx, version, commit, date)
}
