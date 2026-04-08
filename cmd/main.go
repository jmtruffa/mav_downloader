package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"mav_cpd_etl/internal/config"
	"mav_cpd_etl/internal/etl"
)

func main() {
	dateStr := flag.String("date", "", "Fecha a consultar en formato YYYY-MM-DD (por defecto: hoy)")
	flag.Parse()

	if *dateStr == "" {
		today := time.Now().Format("2006-01-02")
		dateStr = &today
	}

	date, err := time.Parse("2006-01-02", *dateStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: fecha inválida %q — usar formato YYYY-MM-DD\n", *dateStr)
		os.Exit(1)
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("error cargando configuración: %v", err)
	}

	if err := etl.Run(cfg, date); err != nil {
		log.Fatalf("error ejecutando ETL: %v", err)
	}
}
