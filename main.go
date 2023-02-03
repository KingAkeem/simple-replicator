package main

import (
	"fmt"
	"simple-replicator/internal/config"
	"simple-replicator/internal/logger"
	"simple-replicator/pkg/db"
	"time"
)

func main() {
	logger.Info("opening database connections...")
	databaseConfigs := config.GetDatabaseList()
	databases := make([]*db.DB, len(databaseConfigs))
	for i, dbConfig := range databaseConfigs {
		databases[i] = &db.DB{Name: dbConfig.Name}
		databases[i].Connect(config.GetDriver())
		defer databases[i].Conn.Close()
	}
	logger.Info("all databases connected...")

	logger.Info("starting replication of databases...")
	for _, src := range databases {
		var tables []*db.Table
		var err error
		if config.GetDriver() == "sqlite3" {
			tables, err = db.GetSQLiteTables(src.Conn)
			if err != nil {
				logger.Error("unable to get tables", "error", err)
				panic(err)
			}
		} else {
			panic("unsupported database")
		}

		for _, dest := range databases {
			if src.Name != dest.Name {
				start := time.Now()
				var schema db.Schema
				if config.GetDriver() == "sqlite3" {
					schema = db.SQLiteSchema{Tables: tables}
				}
				logger.Debug("source", "name", src.Name, "stats", src.Conn.Stats())
				logger.Debug("dest", "name", dest.Name, "stats", dest.Conn.Stats())
				numInserts := db.Replicate(schema, src, dest)
				logger.Info("replication completed successfully",
					"source", src.Name,
					"destination", dest.Name,
					"number of tables", len(tables),
					"number of inserts", numInserts,
					"time to complete", fmt.Sprintf("%v", time.Since(start)),
				)
			}
		}
	}
	logger.Info("databases successfully replicated...")
}
