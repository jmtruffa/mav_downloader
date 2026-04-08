package parser

import (
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

// Row represents a single parsed row from the MAV CPD tasas CSV.
type Row struct {
	FechaConsulta  time.Time
	Dias           int
	Vencimiento    time.Time
	Deposito       time.Time
	TipoInstr      string
	Segmento       string
	PlazoLiq       string
	Moneda         string
	MonedaLiq      string
	MontoNominal   float64
	MontoLiquidado float64
	TasaMax        float64
	TasaMin        float64
	TasaProm       float64
	CantInstr      int
	RawRowHash     string
}

// knownHeaders lists the expected CSV header names (trimmed, lowercased).
// Note: there is no "fecha" column; fecha_consulta comes from the --date CLI argument.
var knownHeaders = []string{
	"dias",
	"vencimiento",
	"deposito",
	"tipo instr.",
	"segmento",
	"plazo liq.",
	"moneda",
	"moneda liq.",
	"monto nominal",
	"monto liquidado",
	"tasa max.",
	"tasa min.",
	"tasa prom.",
	"cant. instr.",
}

// Parse parses the raw CSV body (semicolon-delimited) and returns parsed rows.
// fechaConsulta is the date passed via --date (there is no fecha column in the CSV).
func Parse(data []byte, fechaConsulta time.Time) ([]Row, error) {
	content := string(data)
	// Remove BOM if present
	content = strings.TrimPrefix(content, "\xef\xbb\xbf")
	content = strings.TrimSpace(content)

	if content == "" {
		log.Println("[PARSER] empty CSV response")
		return nil, nil
	}

	reader := csv.NewReader(strings.NewReader(content))
	reader.Comma = ';'
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	// Allow variable number of fields per record to handle trailing separators
	reader.FieldsPerRecord = -1

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parsing CSV: %w", err)
	}

	if len(records) < 2 {
		log.Println("[PARSER] CSV has no data rows (only header or empty)")
		return nil, nil
	}

	// Build header index map
	headerIdx := make(map[string]int)
	for i, h := range records[0] {
		normalized := strings.ToLower(strings.TrimSpace(h))
		if normalized == "" {
			continue // ignore empty trailing column
		}
		headerIdx[normalized] = i
	}

	// Verify all known headers exist
	for _, kh := range knownHeaders {
		if _, ok := headerIdx[kh]; !ok {
			return nil, fmt.Errorf("missing expected header %q in CSV", kh)
		}
	}

	var rows []Row
	for lineNum, record := range records[1:] {
		row, err := parseRecord(record, headerIdx, fechaConsulta, lineNum+2)
		if err != nil {
			log.Printf("[PARSER] skipping line %d: %v", lineNum+2, err)
			continue
		}
		rows = append(rows, row)
	}

	log.Printf("[PARSER] parsed %d rows from %d data lines", len(rows), len(records)-1)
	return rows, nil
}

func parseRecord(fields []string, idx map[string]int, fechaConsulta time.Time, lineNum int) (Row, error) {
	get := func(name string) string {
		i, ok := idx[name]
		if !ok || i >= len(fields) {
			return ""
		}
		return strings.TrimSpace(fields[i])
	}

	var r Row
	var err error

	r.FechaConsulta = fechaConsulta

	// dias
	r.Dias, err = parseIntField(get("dias"))
	if err != nil {
		return r, fmt.Errorf("dias: %w", err)
	}

	// vencimiento
	r.Vencimiento, err = parseDate(get("vencimiento"))
	if err != nil {
		return r, fmt.Errorf("vencimiento: %w", err)
	}

	// deposito
	r.Deposito, err = parseDate(get("deposito"))
	if err != nil {
		return r, fmt.Errorf("deposito: %w", err)
	}

	r.TipoInstr = get("tipo instr.")
	r.Segmento = get("segmento")
	r.PlazoLiq = get("plazo liq.")
	r.Moneda = get("moneda")
	r.MonedaLiq = get("moneda liq.")

	r.MontoNominal, err = parseDecimal(get("monto nominal"))
	if err != nil {
		return r, fmt.Errorf("monto_nominal: %w", err)
	}

	r.MontoLiquidado, err = parseDecimal(get("monto liquidado"))
	if err != nil {
		return r, fmt.Errorf("monto_liquidado: %w", err)
	}

	r.TasaMax, err = parseDecimal(get("tasa max."))
	if err != nil {
		return r, fmt.Errorf("tasa_max: %w", err)
	}

	r.TasaMin, err = parseDecimal(get("tasa min."))
	if err != nil {
		return r, fmt.Errorf("tasa_min: %w", err)
	}

	r.TasaProm, err = parseDecimal(get("tasa prom."))
	if err != nil {
		return r, fmt.Errorf("tasa_prom: %w", err)
	}

	r.CantInstr, err = parseIntField(get("cant. instr."))
	if err != nil {
		return r, fmt.Errorf("cant_instr: %w", err)
	}

	// Build deterministic hash from the normalized row
	r.RawRowHash = hashRow(r)

	return r, nil
}

// parseDate parses DD/MM/YYYY format.
func parseDate(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("empty date")
	}
	t, err := time.Parse("02/01/2006", s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid date %q: %w", s, err)
	}
	return t, nil
}

// parseDecimal parses a number that uses comma as decimal separator.
// Example: "24809679,17" -> 24809679.17
func parseDecimal(s string) (float64, error) {
	if s == "" {
		return 0, nil
	}
	// Remove thousands separator (dot) if present, then replace comma with dot
	s = strings.ReplaceAll(s, ".", "")
	s = strings.ReplaceAll(s, ",", ".")
	return strconv.ParseFloat(s, 64)
}

// parseIntField parses an integer, handling possible decimal commas (e.g. "1,00" -> 1).
func parseIntField(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	// Try direct parse first
	if v, err := strconv.Atoi(s); err == nil {
		return v, nil
	}
	// May come as decimal with comma
	f, err := parseDecimal(s)
	if err != nil {
		return 0, fmt.Errorf("invalid integer %q: %w", s, err)
	}
	return int(f), nil
}

// hashRow generates a deterministic SHA-256 hash from the normalized row fields.
func hashRow(r Row) string {
	data := fmt.Sprintf("%s|%d|%s|%s|%s|%s|%s|%s|%s|%f|%f|%f|%f|%f|%d",
		r.FechaConsulta.Format("2006-01-02"),
		r.Dias,
		r.Vencimiento.Format("2006-01-02"),
		r.Deposito.Format("2006-01-02"),
		r.TipoInstr,
		r.Segmento,
		r.PlazoLiq,
		r.Moneda,
		r.MonedaLiq,
		r.MontoNominal,
		r.MontoLiquidado,
		r.TasaMax,
		r.TasaMin,
		r.TasaProm,
		r.CantInstr,
	)
	h := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", h)
}
