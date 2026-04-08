#!/usr/bin/env bash
set -euo pipefail

START_DATE="2026-01-03"
END_DATE="2026-04-07"
SLEEP_SECONDS=301
BIN="./mav_cpd_etl"

current_date="$START_DATE"

while true; do
    # día de la semana (1=lunes ... 7=domingo)
    day_of_week=$(date -j -f "%Y-%m-%d" "$current_date" "+%u")

    if [[ "$day_of_week" -le 5 ]]; then
        echo "Ejecutando ETL para $current_date"
        "$BIN" --date "$current_date"
        echo "Esperando $SLEEP_SECONDS segundos..."
        sleep "$SLEEP_SECONDS"
    else
        echo "Saltando fin de semana: $current_date"
    fi

    # salir si llegamos al final
    if [[ "$current_date" == "$END_DATE" ]]; then
        echo "Proceso terminado en $END_DATE"
        break
    fi

    # sumar 1 día (compatible macOS)
    current_date=$(date -j -v+1d -f "%Y-%m-%d" "$current_date" "+%Y-%m-%d")
done
