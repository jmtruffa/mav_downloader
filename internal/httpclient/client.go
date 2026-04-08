package httpclient

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

const (
	maxRetries    = 3
	retryDelay    = 5 * time.Second
	requestTimeout = 60 * time.Second
)

// FetchCSV downloads the CSV for a given date from the MAV endpoint.
// dateStr must be in DD/MM/AA format.
func FetchCSV(user, pass, dateStr string) ([]byte, error) {
	baseURL := "https://trading.mav-sa.com.ar/cgi-bin/wspd_cgi.sh/WService%3Dwsbroker1/cpd-tasas-csv.r"

	client := &http.Client{Timeout: requestTimeout}

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest("GET", baseURL, nil)
		if err != nil {
			return nil, fmt.Errorf("building request: %w", err)
		}

		q := req.URL.Query()
		q.Set("mode", "ws")
		q.Set("id", user)
		q.Set("password", pass)
		q.Set("fecha", dateStr)
		req.URL.RawQuery = q.Encode()

		log.Printf("[HTTP] attempt %d/%d — GET %s (fecha=%s)", attempt, maxRetries, baseURL, dateStr)

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("request failed: %w", err)
			log.Printf("[HTTP] attempt %d failed: %v", attempt, lastErr)
			if attempt < maxRetries {
				time.Sleep(retryDelay)
			}
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			lastErr = fmt.Errorf("reading response body: %w", err)
			log.Printf("[HTTP] attempt %d failed reading body: %v", attempt, lastErr)
			if attempt < maxRetries {
				time.Sleep(retryDelay)
			}
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
			log.Printf("[HTTP] attempt %d got status %d", attempt, resp.StatusCode)
			if attempt < maxRetries {
				time.Sleep(retryDelay)
			}
			continue
		}

		log.Printf("[HTTP] success — received %d bytes", len(body))
		return body, nil
	}

	return nil, fmt.Errorf("all %d attempts failed: %w", maxRetries, lastErr)
}
