package etl

import (
	"fmt"
	"log"
	"time"

	"mav_cpd_etl/internal/config"
	"mav_cpd_etl/internal/httpclient"
	"mav_cpd_etl/internal/parser"
	"mav_cpd_etl/internal/storage"
)

// Run executes the full ETL pipeline for a single date.
func Run(cfg *config.Config, date time.Time) error {
	// Convert date to DD/MM/AA for the endpoint
	mavDate := date.Format("02/01/06")
	log.Printf("[ETL] starting — date=%s (endpoint fecha=%s)", date.Format("2006-01-02"), mavDate)

	// 1. Connect to PostgreSQL
	store, err := storage.New(cfg.PostgresDSN())
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()

	// 2. Fetch CSV from endpoint
	body, err := httpclient.FetchCSV(cfg.MAVUser, cfg.MAVPass, mavDate)
	if err != nil {
		return fmt.Errorf("fetching CSV: %w", err)
	}

	// 3. Parse CSV
	rows, err := parser.Parse(body, date)
	if err != nil {
		return fmt.Errorf("parsing CSV: %w", err)
	}

	if len(rows) == 0 {
		log.Printf("[ETL] no data rows for date %s — nothing to insert", date.Format("2006-01-02"))
		return nil
	}

	log.Printf("[ETL] parsed %d rows", len(rows))

	// 4. Upsert into PostgreSQL
	stats, err := store.UpsertRows(rows)
	if err != nil {
		return fmt.Errorf("upserting rows: %w", err)
	}

	log.Printf("[ETL] done — date=%s read=%d inserted=%d updated=%d skipped=%d errors=%d",
		date.Format("2006-01-02"),
		stats.Read, stats.Inserted, stats.Updated, stats.Skipped, stats.Errors,
	)

	return nil
}
