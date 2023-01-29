package main

import (
	"database/sql"
	"fmt"
	"os"
	"simple-replicator/internal/logger"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v3"
)

// DB is the configuration for a database
type DB struct {
	Name   string  `yaml:"name"`
	Driver string  `yaml:"driver,omitempty"`
	Conn   *sql.DB `yaml:"-"`
}

type Config struct {
	Databases []*DB `yaml:"databases"`
}

// connects to database instance
func (d *DB) connect() {
	// open database connection based on configuration
	if d.Driver == "sqlite" || strings.TrimSpace(d.Driver) == "" {
		logger.Debug("setting up sqlite database connection", "name", d.Name)
		db, err := sql.Open("sqlite3", d.Name)
		if err != nil {
			logger.Error("unable to open sqlite database", "error", err)
			panic(err)
		}
		d.Conn = db
	}
	logger.Debug("successfully connected", "name", d.Name)
}

// creates output table for data placement
func createTable(db *sql.DB, name, columns string) error {
	// used for testing
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(%s)`, name, columns))
	if err != nil {
		panic(err)
	}
	return err
}

func getTables(db *sql.DB) ([]*SQLiteTable, error) {
	logger.Debug("getting tables")
	rows, err := db.Query("SELECT type, name, tbl_name, rootpage, sql FROM sqlite_master WHERE type='table'")
	if err != nil {
		return nil, err
	}

	tables := []*SQLiteTable{}
	for rows.Next() {
		t := new(SQLiteTable)
		err = rows.Scan(
			&t.Type,
			&t.Name,
			&t.TableName,
			&t.RootPage,
			&t.SQL,
		)
		if err != nil {
			logger.Error("unable to scan row", "error", err)
			continue
		}
		tables = append(tables, t)
	}

	for _, t := range tables {
		query := fmt.Sprintf("SELECT cid, name, type, dflt_value, pk FROM pragma_table_info('%s')", t.TableName)
		rows, err := db.Query(query)
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			column := new(SQLiteColumn)
			err = rows.Scan(
				&column.CID,
				&column.Name,
				&column.Type,
				&column.DefaultValue,
				&column.PrimaryKey,
			)
			if err != nil {
				logger.Error("unable to scan row", "error", err)
				continue
			}
			t.Columns = append(t.Columns, column)
		}
	}

	logger.Debug("tables retrieved", "tables", tables)
	return tables, nil
}

func replicate(tables []*SQLiteTable, src, dest *DB) int {
	numInserts := 0

	for _, t := range tables {
		columns := make([]string, len(t.Columns))
		for i, column := range t.Columns {
			columns[i] = column.Name
		}
		createTable(dest.Conn, t.TableName, strings.Join(columns, ",")) // create tables for testing
	}

	insertTx, err := dest.Conn.Begin()
	if err != nil {
		panic(err)
	}

	// each table should be able to be replicated in parallel
	for _, t := range tables {
		cells := make([]interface{}, len(t.Columns))
		columns := make([]string, len(t.Columns))
		for i, column := range t.Columns {
			cells[i] = new(sql.RawBytes)
			columns[i] = column.Name
		}

		// pull source data
		query := fmt.Sprintf("SELECT * FROM %s", t.TableName)
		rows, err := src.Conn.Query(query)
		if err != nil {
			logger.Error("unable to perform query", "error", err)
			panic(err)
		}

		// read source data
		for rows.Next() {
			err = rows.Scan(cells...)
			if err != nil {
				logger.Error("unable to scan row", "error", err)
				panic(err)
			}

			// build row
			row := []string{}
			for _, cell := range cells {
				cellStr := string(*cell.(*sql.RawBytes))
				if strings.Contains(cellStr, `"`) {
					row = append(row, fmt.Sprintf(`'%s'`, cellStr))
				} else {
					row = append(row, fmt.Sprintf(`"%s"`, cellStr))
				}
			}

			columnNames := strings.Join(columns, ",")
			rowValues := strings.Join(row, ",")
			statement := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", t.TableName, columnNames, rowValues)

			// create key value pairs for WHERE clauses
			pairs := make([]string, 0)
			for i, r := range row {
				if strings.TrimSpace(r) != `""` {
					pairs = append(pairs, fmt.Sprintf("%s = %s", columns[i], r))
				}
			}
			filter := strings.Join(pairs, " and ")
			// construct query
			query = fmt.Sprintf(`SELECT * FROM %s where %s`, t.TableName, filter)
			existingRows, err := dest.Conn.Query(query)
			if err != nil {
				panic(err)
			}

			// check if row exists
			existingCells := make([]interface{}, len(t.Columns))
			for i := range t.Columns {
				existingCells[i] = new(sql.RawBytes)
			}
			row = []string{}
			for existingRows.Next() {
				err = existingRows.Scan(existingCells...)
				if err != nil {
					logger.Error("unable to scan row", "error", err)
					panic(err)
				}
				for _, cell := range existingCells {
					if cell != nil {
						cellStr := string(*cell.(*sql.RawBytes))
						if strings.Contains(cellStr, `"`) {
							row = append(row, fmt.Sprintf(`'%s'`, cellStr))
						} else {
							row = append(row, fmt.Sprintf(`"%s"`, cellStr))
						}
					}
				}

				if existingCells[0] != nil {
					break
				}
			}

			// if elements exist, skip
			if len(row) > 0 {
				logger.Debug("skipping insert", "statement", statement)
				continue
			}

			logger.Debug("Inserting", "statement", statement)
			// insert data
			_, err = insertTx.Exec(statement)
			if err != nil {
				panic(err)
			}

			numInserts++
			logger.Debug("rows successfully inserted", "number inserted", numInserts)
		}
		rows.Close()
	}

	err = insertTx.Commit()
	if err != nil {
		panic(err)
	}
	return numInserts
}

type SQLiteTable struct {
	Type      string
	Name      string
	TableName string
	RootPage  string
	SQL       string
	Columns   []*SQLiteColumn
}

type SQLiteColumn struct {
	CID          int64
	Name         string
	Type         string
	DefaultValue sql.NullString
	PrimaryKey   int64
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
		db.connect()
		defer db.Conn.Close()
	}
	logger.Info("all databases connected...")

	logger.Info("starting replication of databases...")
	for _, src := range c.Databases {
		tables, err := getTables(src.Conn)
		if err != nil {
			logger.Error("unable to get tables", "error", err)
			panic(err)
		}

		for _, dest := range c.Databases {
			if src.Name != dest.Name {
				start := time.Now()
				numInserts := replicate(tables, src, dest)
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
