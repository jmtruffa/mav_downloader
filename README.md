# MAV CPD Tasas — ETL

CLI en Go que descarga tasas del endpoint MAV-AL-CPD y las ingesta en PostgreSQL.

## Variables de entorno

| Variable | Requerida | Descripción |
|---|---|---|
| `MAV_API_USER` | Sí | Usuario del endpoint MAV |
| `MAV_API_PASS` | Sí | Contraseña del endpoint MAV |
| `POSTGRES_USER` | Sí | Usuario PostgreSQL |
| `POSTGRES_PASSWORD` | Sí | Contraseña PostgreSQL |
| `POSTGRES_HOST` | No | Host PostgreSQL (default: `localhost`) |
| `POSTGRES_PORT` | No | Puerto PostgreSQL (default: `5432`) |
| `POSTGRES_DB` | Sí | Base de datos PostgreSQL |

## Compilar y ejecutar

```bash
go build -o mav_cpd_etl ./cmd/

# Ejecutar para una fecha puntual
./mav_cpd_etl --date 2026-04-07
```

## Esquema de tabla

```sql
CREATE TABLE mav_cpd_tasas (
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
```

## Estrategia de deduplicación

- Se genera un hash SHA-256 determinístico sobre todos los campos normalizados de cada fila.
- Se define una restricción `UNIQUE (fecha_consulta, raw_row_hash)`.
- El `INSERT` usa `ON CONFLICT ... DO UPDATE` (upsert): si la fila ya existe para esa fecha y hash, se actualizan los campos y el `updated_at`.
- Esto hace que cada ejecución sea idempotente: correr el mismo día múltiples veces no genera duplicados.

## Supuestos de parseo del CSV

- Delimitador: `;` (punto y coma).
- La cabecera puede traer una columna vacía al final por separador terminal; se ignora.
- Todos los campos se trimmean de espacios antes de parsearse.
- Fechas en formato `DD/MM/YYYY`.
- Números decimales usan coma como separador decimal (ej: `24809679,17`).
- Números pueden usar punto como separador de miles (se remueve antes de parsear).
- Campos enteros que vengan como decimal (ej: `1,00`) se truncan a entero.
- Filas con errores de parseo se loguean y se omiten (no interrumpen el proceso).

## Carga histórica

Para procesar muchas fechas, respetar una espera mínima de **330 segundos** entre requests:

```bash
for date in 2026-03-01 2026-03-02 2026-03-03; do
    ./mav_cpd_etl --date "$date"
    sleep 330
done
```
