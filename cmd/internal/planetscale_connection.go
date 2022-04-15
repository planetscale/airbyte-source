package internal

import (
	"context"
	"fmt"
	"github.com/go-sql-driver/mysql"
	psdbdatav1 "github.com/planetscale/edge-gateway/proto/psdb/data_v1"
	"io"
	"os"
	"time"
)

type PlanetScaleConnection struct {
	Host                  string `json:"host"`
	Database              string `json:"database"`
	Username              string `json:"username"`
	Password              string `json:"password"`
	SyncDurationInMinutes int    `json:"sync_duration"`
	DatabaseAccessor      PlanetScaleDatabase
}

func (psc PlanetScaleConnection) DSN() string {
	config := mysql.NewConfig()
	config.Net = "tcp"
	config.Addr = psc.Host
	config.User = psc.Username
	config.DBName = psc.Database
	config.Passwd = psc.Password
	if useSecureConnection() {
		config.TLSConfig = "true"
		config.DBName = fmt.Sprintf("%v@replica", psc.Database)
	}
	return config.FormatDSN()
}

func (psc PlanetScaleConnection) Check() error {
	_, err := psc.DatabaseAccessor.CanConnect(context.Background(), psc)
	if err != nil {
		return err
	}
	return nil
}

func (psc PlanetScaleConnection) DiscoverSchema() (c Catalog, err error) {
	return psc.DatabaseAccessor.DiscoverSchema(context.Background(), psc)
}

func (psc PlanetScaleConnection) Read(w io.Writer, table ConfiguredStream, tc *psdbdatav1.TableCursor) (*SerializedCursor, error) {
	return psc.DatabaseAccessor.Read(context.Background(), w, psc, table, psc.getMaxReadDurationInMinutes(), tc)
}

func (psc PlanetScaleConnection) ListShards(ctx context.Context) ([]string, error) {
	return psc.DatabaseAccessor.ListShards(context.Background(), psc)
}

func (ps PlanetScaleConnection) getMaxReadDurationInMinutes() time.Duration {
	if ps.SyncDurationInMinutes == 0 {
		return 2 * time.Minute
	}

	return time.Duration(ps.SyncDurationInMinutes) * time.Minute
}

func useSecureConnection() bool {
	e2eTestRun, found := os.LookupEnv("PS_END_TO_END_TEST_RUN")
	if found && (e2eTestRun == "yes" ||
		e2eTestRun == "y" ||
		e2eTestRun == "true" ||
		e2eTestRun == "1") {
		return false
	}

	return true
}
