package db

import (
	"database/sql"
	"fmt"
	"simple-replicator/internal/logger"
	"strings"
)

// DB is the configuration for a database
type DB struct {
	Name string  `yaml:"name"`
	Conn *sql.DB `yaml:"-"`
}

type Schema interface {
	GetTables() []*Table
}

type IColumn interface {
	GetName() string
}

type ITable interface {
	GetTableName() string
	GetColumns() []*Column
	SetColumns(columns []*Column)
	GetColumnNames() []string
}

type Column struct {
	IColumn
}

type Table struct {
	ITable
}

// connects to database instance
func (d *DB) Connect(driver string) {
	// open database connection based on configuration
	if driver == "sqlite3" {
		db, err := connectSQLite(d.Name)
		if err != nil {
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

func Replicate(schema Schema, src, dest *DB) int {
	numInserts := 0
	tables := schema.GetTables()
	for _, t := range tables {
		createTable(dest.Conn, t.GetTableName(), strings.Join(t.GetColumnNames(), ",")) // create tables for testing
	}

	insertTx, err := dest.Conn.Begin()
	if err != nil {
		panic(err)
	}

	// each table should be able to be replicated in parallel
	for _, t := range tables {
		columns := t.GetColumnNames()
		cells := make([]interface{}, len(columns))
		for i := range columns {
			cells[i] = new(sql.RawBytes)
		}

		// pull source data
		query := fmt.Sprintf("SELECT * FROM %s", t.GetTableName())
		rows, err := src.Conn.Query(query)
		if err != nil {
			logger.Error("unable to perform query", "error", err)
			panic(err)
		}
		defer rows.Close()

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
			statement := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", t.GetTableName(), columnNames, rowValues)

			// create key value pairs for WHERE clauses
			pairs := make([]string, 0)
			for i, r := range row {
				if strings.TrimSpace(r) != `""` {
					pairs = append(pairs, fmt.Sprintf("%s = %s", columns[i], r))
				}
			}
			filter := strings.Join(pairs, " and ")
			// construct query
			query = fmt.Sprintf(`SELECT * FROM %s where %s`, t.GetTableName(), filter)
			existingRows, err := dest.Conn.Query(query)
			if err != nil {
				panic(err)
			}
			defer existingRows.Close()

			// check if row exists
			existingCells := make([]interface{}, len(columns))
			for i := range columns {
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
	}

	err = insertTx.Commit()
	if err != nil {
		panic(err)
	}
	return numInserts
}
