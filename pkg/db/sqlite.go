package db

import (
	"database/sql"
	"fmt"
	"simple-replicator/internal/logger"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteSchema struct {
	Tables []*Table
}

func (s SQLiteSchema) GetTables() []*Table {
	return s.Tables
}

type SQLiteTable struct {
	Type        string
	Name        string
	TableName   string
	RootPage    string
	SQL         string
	Columns     []*Column
	ColumnNames []string
}

func (t *SQLiteTable) SetColumns(columns []*Column) {
	t.Columns = columns
}

func (t *SQLiteTable) GetColumns() []*Column {
	return t.Columns
}

func (t *SQLiteTable) GetTableName() string {
	return t.TableName
}

func (t *SQLiteTable) GetColumnNames() []string {
	// if called previously, return column names
	if len(t.ColumnNames) > 0 {
		return t.ColumnNames
	}

	columns := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		columns[i] = c.GetName()
	}
	t.ColumnNames = columns
	return t.ColumnNames
}

func (c *SQLiteColumn) GetName() string {
	return c.Name
}

type SQLiteColumn struct {
	CID          int64
	Name         string
	Type         string
	DefaultValue sql.NullString
	PrimaryKey   int64
}

func connectSQLite(name string) (*sql.DB, error) {
	logger.Debug("setting up sqlite database connection", "name", name)
	db, err := sql.Open("sqlite3", name)
	if err != nil {
		logger.Error("unable to open sqlite database", "error", err)
		panic(err)
	}
	return db, nil
}

func GetSQLiteTables(db *sql.DB) ([]*Table, error) {
	logger.Debug("getting tables")
	rows, err := db.Query("SELECT type, name, tbl_name, rootpage, sql FROM sqlite_master WHERE type='table'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := []*Table{}
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
		tables = append(tables, &Table{t})
	}

	for _, t := range tables {
		query := fmt.Sprintf("SELECT cid, name, type, dflt_value, pk FROM pragma_table_info('%s')", t.GetTableName())
		rows, err := db.Query(query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		columns := t.GetColumns()
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
			columns = append(columns, &Column{column})
		}
		t.SetColumns(columns)
	}

	logger.Debug("tables retrieved", "tables", tables)
	return tables, nil
}
