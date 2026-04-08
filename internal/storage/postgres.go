package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"

	"mav_cpd_etl/internal/parser"
)

const createTableSQL = `
CREATE TABLE IF NOT EXISTS mav_cpd_tasas (
    id              BIGSERIAL PRIMARY KEY,
    fecha_consulta  DATE NOT NULL,
    dias            INTEGER NOT NULL,
    vencimiento     DATE NOT NULL,
    deposito        DATE NOT NULL,
    tipo_instr      TEXT NOT NULL,
    segmento        TEXT NOT NULL,
    plazo_liq       TEXT NOT NULL,
    moneda          TEXT NOT NULL,
    moneda_liq      TEXT NOT NULL,
    monto_nominal   NUMERIC NOT NULL,
    monto_liquidado NUMERIC NOT NULL,
    tasa_max        NUMERIC NOT NULL,
    tasa_min        NUMERIC NOT NULL,
    tasa_prom       NUMERIC NOT NULL,
    cant_instr      INTEGER NOT NULL,
    raw_row_hash    TEXT NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_fecha_hash UNIQUE (fecha_consulta, raw_row_hash)
);
`

const upsertSQL = `
INSERT INTO mav_cpd_tasas (
    fecha_consulta, dias, vencimiento, deposito,
    tipo_instr, segmento, plazo_liq, moneda, moneda_liq,
    monto_nominal, monto_liquidado,
    tasa_max, tasa_min, tasa_prom,
    cant_instr, raw_row_hash, created_at, updated_at
) VALUES (
    $1, $2, $3, $4,
    $5, $6, $7, $8, $9,
    $10, $11,
    $12, $13, $14,
    $15, $16, $17, $18
)
ON CONFLICT (fecha_consulta, raw_row_hash)
DO UPDATE SET
    dias            = EXCLUDED.dias,
    vencimiento     = EXCLUDED.vencimiento,
    deposito        = EXCLUDED.deposito,
    tipo_instr      = EXCLUDED.tipo_instr,
    segmento        = EXCLUDED.segmento,
    plazo_liq       = EXCLUDED.plazo_liq,
    moneda          = EXCLUDED.moneda,
    moneda_liq      = EXCLUDED.moneda_liq,
    monto_nominal   = EXCLUDED.monto_nominal,
    monto_liquidado = EXCLUDED.monto_liquidado,
    tasa_max        = EXCLUDED.tasa_max,
    tasa_min        = EXCLUDED.tasa_min,
    tasa_prom       = EXCLUDED.tasa_prom,
    cant_instr      = EXCLUDED.cant_instr,
    updated_at      = EXCLUDED.updated_at
RETURNING (xmax = 0) AS inserted
`

// Stats holds the result counts of an upsert batch.
type Stats struct {
	Read     int
	Inserted int
	Updated  int
	Skipped  int
	Errors   int
}

// Store wraps a PostgreSQL connection.
type Store struct {
	db *sql.DB
}

// New opens a connection to PostgreSQL and ensures the schema exists.
func New(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	if _, err := db.ExecContext(ctx, createTableSQL); err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}

	log.Println("[DB] connected and schema ensured")
	return &Store{db: db}, nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// UpsertRows inserts or updates all rows in a single transaction.
func (s *Store) UpsertRows(rows []parser.Row) (Stats, error) {
	var stats Stats
	stats.Read = len(rows)

	if len(rows) == 0 {
		return stats, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return stats, fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, upsertSQL)
	if err != nil {
		return stats, fmt.Errorf("preparing upsert: %w", err)
	}
	defer stmt.Close()

	now := time.Now()

	for i, r := range rows {
		var inserted bool
		err := stmt.QueryRowContext(ctx,
			r.FechaConsulta, r.Dias, r.Vencimiento, r.Deposito,
			r.TipoInstr, r.Segmento, r.PlazoLiq, r.Moneda, r.MonedaLiq,
			r.MontoNominal, r.MontoLiquidado,
			r.TasaMax, r.TasaMin, r.TasaProm,
			r.CantInstr, r.RawRowHash, now, now,
		).Scan(&inserted)

		if err != nil {
			log.Printf("[DB] error upserting row %d: %v", i+1, err)
			stats.Errors++
			continue
		}

		if inserted {
			stats.Inserted++
		} else {
			stats.Updated++
		}
	}

	if err := tx.Commit(); err != nil {
		return stats, fmt.Errorf("committing transaction: %w", err)
	}

	return stats, nil
}
