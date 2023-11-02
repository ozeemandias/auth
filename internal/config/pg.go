package config

import (
	"errors"
	"fmt"
	"os"
)

const (
	pgHostEnvName = "POSTGRES_HOST"
	pgPortEnvName = "POSTGRES_PORT"
	phDBEnvName   = "POSTGRES_DB"
	pgUserEnvName = "POSTGRES_USER"
	//nolint:gosec
	pgPasswordEnvName = "POSTGRES_PASSWORD"
)

//nolint:revive
type PGConfig interface {
	DSN() string
}

type pgConfig struct {
	host     string
	port     string
	db       string
	user     string
	password string
}

//nolint:revive
func NewPGConfig() (PGConfig, error) {
	host := os.Getenv(pgHostEnvName)
	if len(host) == 0 {
		return nil, errors.New("pg host not found")
	}

	port := os.Getenv(pgPortEnvName)
	if len(port) == 0 {
		return nil, errors.New("pg port not found")
	}

	db := os.Getenv(phDBEnvName)
	if len(db) == 0 {
		return nil, errors.New("pg db not found")
	}

	user := os.Getenv(pgUserEnvName)
	if len(user) == 0 {
		return nil, errors.New("pg user not found")
	}

	password := os.Getenv(pgPasswordEnvName)
	if len(password) == 0 {
		return nil, errors.New("pg password not found")
	}

	return &pgConfig{
		host:     host,
		port:     port,
		db:       db,
		user:     user,
		password: password,
	}, nil
}

func (cfg *pgConfig) DSN() string {
	return fmt.Sprintf("host=%v port=%v dbname=%v user=%v password=%v sslmode=disable", cfg.host, cfg.port, cfg.db, cfg.user, cfg.password)
}
