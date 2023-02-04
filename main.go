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
		tables, err := src.GetTables()
		if err != nil {
			logger.Fatal("unable to get tables", "error", err)
		}

		for _, dest := range databases {
			if src.Name != dest.Name {
				start := time.Now()
				schema, err := src.GetSchema()
				if err != nil {
					panic(err)
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
