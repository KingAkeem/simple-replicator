package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type databaseConfig struct {
	Name string `yaml:"name"`
}

type config struct {
	LogLevel  string           `yaml:"loglevel"`
	Driver    string           `yaml:"driver"`
	Databases []databaseConfig `yaml:"databases"`
}

var c *config = &config{}

func GetDriver() string {
	return c.Driver
}

func GetLevel() string {
	return c.LogLevel
}

func GetDatabaseList() []databaseConfig {
	return c.Databases
}

func init() {
	f, err := os.Open("config.yaml")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	yaml.NewDecoder(f).Decode(c)
}
