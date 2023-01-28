package main

import (
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
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
		log.Println("setting up sqlite database connection")
		db, err := sql.Open("sqlite3", d.Name)
		if err != nil {
			panic(err)
		}
		d.Conn = db
	}
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

func replicate(tables []*SQLiteTable, src, dest, session *DB) int {
	numInserts := 0
	// each table should be able to be replicated in parallel
	for _, t := range tables {
		cells := make([]interface{}, len(t.Columns))
		columns := make([]string, len(t.Columns))
		for i, column := range t.Columns {
			cells[i] = new(sql.RawBytes)
			columns[i] = column.Name
		}
		createTable(dest.Conn, t.TableName, strings.Join(columns, ","))
		query := fmt.Sprintf("SELECT * FROM %s", t.TableName)
		rows, err := src.Conn.Query(query)
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			err = rows.Scan(cells...)
			if err != nil {
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
			// hash insert
			statementHash, err := getHash(statement)
			if err != nil {
				panic(err)
			}

			// this insert shouldn't be ran on src
			session.Conn.Exec("INSERT INTO session VALUES($1, $2)", statementHash, src.Name)

			// check session for operation, insert if it hasn't been done before
			sessionRow := session.Conn.QueryRow("SELECT hash FROM session WHERE hash = $1 and origin = $2", statementHash, dest.Name)
			var hash sql.NullString
			sessionRow.Scan(&hash)
			if hash.Valid { // this insertion has already been performed
				log.Printf("SKIPPED INSERT [%s]\n", statement)
				continue
			} else {
				session.Conn.Exec("INSERT INTO session VALUES($1, $2)", statementHash, dest.Name)
			}

			log.Println(statement)
			// insert data
			_, err = dest.Conn.Exec(statement)
			if err != nil {
				panic(err)
			}
			numInserts++
		}
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

func getHash(text string) (string, error) {
	h := sha1.New()
	_, err := h.Write([]byte(text))
	if err != nil {
		return "", err
	}
	sha1_hash := hex.EncodeToString(h.Sum(nil))
	return sha1_hash, nil
}

func main() {
	f, err := os.Open("config.yaml")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	c := new(Config)
	yaml.NewDecoder(f).Decode(c)

	for _, db := range c.Databases {
		log.Printf("connecting to %s...\n", db.Name)
		db.connect()
		defer db.Conn.Close()
		log.Printf("%s database connection established successfully\n", db.Name)
	}

	session := &DB{Name: "session"}
	session.connect()
	defer session.Conn.Close()
	defer os.Remove("session") // removed after session is over

	session.Conn.Exec("CREATE TABLE IF NOT EXISTS session(hash, origin)")

	for _, src := range c.Databases {
		log.Printf("retrieving tables for %s...", src.Name)
		tables, err := getTables(src.Conn)
		if err != nil {
			panic(err)
		}
		log.Println("retrieved tables: ", tables)
		for _, dest := range c.Databases {
			if src.Name != dest.Name {
				log.Printf("starting replication from %s to %s\n", src.Name, dest.Name)
				numInserts := replicate(tables, src, dest, session)
				log.Printf("number of inserts performed: %d\n", numInserts)
			}
		}
	}
}
