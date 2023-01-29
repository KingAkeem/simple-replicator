package main

import (
	"fmt"
	"os"
	"simple-replicator/internal/logger"
	"simple-replicator/pkg/db"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Databases []*db.DB `yaml:"databases"`
}

func main() {
	logger.Info("reading configuration file...")
	f, err := os.Open("config.yaml")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	c := new(Config)
	yaml.NewDecoder(f).Decode(c)
	logger.Info("configuration file loaded successfully")

	logger.Info("opening database connections...")
	for _, db := range c.Databases {
		db.Connect()
		defer db.Conn.Close()
	}
	logger.Info("all databases connected...")

	logger.Info("starting replication of databases...")
	for _, src := range c.Databases {
		tables, err := db.GetSQLiteTables(src.Conn)
		if err != nil {
			logger.Error("unable to get tables", "error", err)
			panic(err)
		}

		for _, dest := range c.Databases {
			if src.Name != dest.Name {
				start := time.Now()
				schema := db.SQLiteSchema{Tables: tables}
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
