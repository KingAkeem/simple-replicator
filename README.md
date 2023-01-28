# data-observer

## Summary
This program is intended to observe data from various data sources.
The current implementation is fairly simple and straightforward, the configuration file is `config.yaml` in the root directory.
The program connects to 2 data sources, one is considered a source and the other is considered a destination.
- source should be the database of interest, meaning it should have the tables containing the relevant information.
- destination will contain the counts of the relevant data. This database along with the tables will be generated, if necessary.
- frequency determines how often data will be observed.
	- an observation is considered to be a read from source and write to destination.

### YAML Configuration
- src - the database instance where data will be read from.
	- driver - the database driver
	- name - the name of the database
- dest - the database instance where data will be written to.
	- driver - the database driver
	- name - the name of the database

- tables - the database tables where data will be read/written. The src tables are expected to exist on its respective instance.
	- src -  database table where the count will be read 
	- dest - the database table where the data will be written
	- frequency - how often the observation will occur in seconds
	- Filter criteria, listed under `filter` (TODO)

### Supported Databases
- sqlite