package config

import (
	"fmt"
	"os"
)

type Config struct {
	MAVUser      string
	MAVPass      string
	PGUser       string
	PGPassword   string
	PGHost       string
	PGDB         string
	PGPort       string
}

func Load() (*Config, error) {
	c := &Config{
		MAVUser:    os.Getenv("MAV_API_USER"),
		MAVPass:    os.Getenv("MAV_API_PASS"),
		PGUser:     os.Getenv("POSTGRES_USER"),
		PGPassword: os.Getenv("POSTGRES_PASSWORD"),
		PGHost:     os.Getenv("POSTGRES_HOST"),
		PGDB:       os.Getenv("POSTGRES_DB"),
		PGPort:     os.Getenv("POSTGRES_PORT"),
	}

	if c.MAVUser == "" {
		return nil, fmt.Errorf("MAV_API_USER is required")
	}
	if c.MAVPass == "" {
		return nil, fmt.Errorf("MAV_API_PASS is required")
	}
	if c.PGUser == "" {
		return nil, fmt.Errorf("POSTGRES_USER is required")
	}
	if c.PGPassword == "" {
		return nil, fmt.Errorf("POSTGRES_PASSWORD is required")
	}
	if c.PGHost == "" {
		c.PGHost = "localhost"
	}
	if c.PGDB == "" {
		return nil, fmt.Errorf("POSTGRES_DB is required")
	}
	if c.PGPort == "" {
		c.PGPort = "5432"
	}

	return c, nil
}

func (c *Config) PostgresDSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.PGUser, c.PGPassword, c.PGHost, c.PGPort, c.PGDB,
	)
}
