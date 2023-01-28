package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v3"
)

// DB is the configuration for a database
type DB struct {
	Name   string  `yaml:"name"`
	Driver string  `yaml:"driver"`
	Conn   *sql.DB `yaml:"-"`
}

// Observation are the observations that will be stored
type Observation struct {
	Src       string `yaml:"src"`
	Dest      string `yaml:"dest"`
	Frequency int    `yaml:"frequency"`
}

// Entire configuration
type Config struct {
	Src          DB            `yaml:"src"`
	Dest         DB            `yaml:"dest"`
	Observations []Observation `yaml:"observations"`
}

// connects to database instance
func (d *DB) connect() {
	// open database connection based on configuration
	if d.Driver == "sqlite" {
		log.Println("setting up sqlite database connection")
		db, err := sql.Open("sqlite3", d.Name)
		if err != nil {
			panic(err)
		}
		d.Conn = db
	}
}

// creates output table for data placement
func createTable(db *sql.DB, name string) error {
	// used for testing
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(id, count, timestamp)`, name))
	if err != nil {
		panic(err)
	}
	return err
}

func getTables(db *sql.DB) ([]*SQLiteTable, error) {
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
			log.Println(err)
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
				log.Println(err)
				continue
			}
			t.Columns = append(t.Columns, column)
		}
	}

	return tables, nil
}

func replicate(tables []*SQLiteTable, src, dest *sql.DB) {
	for _, t := range tables {
		cells := make([]interface{}, len(t.Columns))
		columns := make([]string, len(t.Columns))
		for i, column := range t.Columns {
			cells[i] = new(sql.RawBytes)
			columns[i] = column.Name
		}
		query := fmt.Sprintf("SELECT * FROM %s", t.TableName)
		rows, err := src.Query(query)
		if err != nil {
			panic(err)
		}

		for rows.Next() {
			err = rows.Scan(cells...)
			if err != nil {
				panic(err)
			}

			row := []string{}
			for _, cell := range cells {
				cellStr := string(*cell.(*sql.RawBytes))
				row = append(row, fmt.Sprintf(`"%s"`, cellStr))
			}

			statement := fmt.Sprintf("INSERT INTO %s VALUES (%s)", t.TableName, strings.Join(row, ","))
			log.Println(statement)
			_, err = dest.Exec(statement)
			if err != nil {
				panic(err)
			}
		}
	}
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
	// read configuration
	log.Println("loading configuration")
	configFile, err := os.Open("config.yaml")
	if err != nil {
		panic(err)
	}
	defer configFile.Close()

	c := new(Config)
	err = yaml.NewDecoder(configFile).Decode(c)
	if err != nil {
		panic(err)
	}
	log.Println("config loaded successfully")

	log.Println("connecting to source...")
	c.Src.connect()
	defer c.Src.Conn.Close()
	log.Println("source database connection established successfully")

	log.Println("retrieving tables...")
	tables, err := getTables(c.Src.Conn)
	if err != nil {
		panic(err)
	}
	log.Println("retrieved tables: ", tables)

	log.Println("connecting to destination...")
	c.Dest.connect()
	defer c.Dest.Conn.Close()
	log.Println("destination database connection established successfully")

	log.Println("beginning replication...")
	replicate(tables, c.Src.Conn, c.Dest.Conn)
	log.Println("replication completed successfully")
}
