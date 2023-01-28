package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

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

// writes data to file for a single observation
func writeObservation(src *sql.DB, dest *sql.DB, obv *Observation) error {
	// get count
	query := fmt.Sprintf("SELECT count(*) FROM %s", obv.Src)
	row := src.QueryRow(query)
	var count int
	err := row.Scan(&count)
	if err != nil {
		return err
	}

	timestamp := time.Now().UTC().String()
	statement := fmt.Sprintf("INSERT INTO %s (id, count, timestamp) VALUES ($2, $3, $4)", obv.Dest)
	_, err = dest.Exec(statement, 1, count, timestamp)
	return err
}

// starts a timer for each observation that runs at intervals specified
func observe(obv *Observation, src, dest *sql.DB) {
	freq := time.Second * time.Duration(obv.Frequency)
	c := time.Tick(freq)
	for now := range c {
		log.Println("writing observation", now.UTC().String())
		err := writeObservation(src, dest, obv)
		if err != nil {
			log.Println(err)
		}
	}
}

// indefinitely writes data for observation based on frequency
func startObserving(src *sql.DB, dest *sql.DB, observations []Observation) {
	for _, obv := range observations {
		observe(&obv, src, dest)
	}
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

func main() {
	// read configuration
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

	c.Dest.connect()
	defer c.Dest.Conn.Close()
	log.Println("destination database connection established successfully")

	c.Src.connect()
	defer c.Src.Conn.Close()
	log.Println("source database connection established successfully")

	// create tables for observations
	for _, obv := range c.Observations {
		createTable(c.Dest.Conn, obv.Dest)
	}

	log.Printf("Source: %+v\n", c.Src)
	log.Printf("Destination: %+v\n", c.Dest)
	log.Println("starting observation...")
	// attempt to write observation data at the frequency specified
	startObserving(c.Src.Conn, c.Dest.Conn, c.Observations)
}
