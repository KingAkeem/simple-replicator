# simple-replicator

simple-replicator is a program that replicates database tables using Golang.

The databases supported are:
- sqlite3
- Postgresql (TODO)
- MySQL (TODO) 

## How To Use
The replicator uses a YAML configuration file to perform replication.
The configuration file should contain the necessary configuration for connecting to the database, depending on the driver.
e.g. sqlite3 only requires a file name while Postgresql requires multiple values
```YAML
loglevel: DEBUG
driver: sqlite3
databases:
  - name: mydb
  - name: test
  - name: example
```
The expected file name is `config.yaml`, the replicator expects the databases to already exist based on the configuration.
(SQLite will automatically create database files for non-existing databases, but at least one needs to exist).
Currently, there is trouble with replicating multiple databases at once, so it's recommended to replicate a single database at a time.
Once this file has been configured as desired, run `go run main.go` to begin replication.
