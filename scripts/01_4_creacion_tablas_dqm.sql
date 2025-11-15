-- Inventario de scripts (control)
CREATE TABLE IF NOT EXISTS dqm_scripts_inventory (
    script_id    SERIAL PRIMARY KEY,
    script_name  TEXT NOT NULL,
    description  TEXT,
    filename     TEXT,
    created_by   TEXT,
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Log de ejecución de scripts (control)
CREATE TABLE IF NOT EXISTS dqm_exec_log (
    log_id         BIGSERIAL PRIMARY KEY,
    script_id      INT REFERENCES dqm_scripts_inventory(script_id),
    started_at     TIMESTAMP WITH TIME ZONE,
    finished_at    TIMESTAMP WITH TIME ZONE,
    status         TEXT,            -- 'RUNNING', 'OK', 'ERROR'
    message        TEXT,
    rows_processed BIGINT,
    extra          JSONB
);

-- DQM: checks por campo
CREATE TABLE IF NOT EXISTS dqm_field_checks (
    check_id       BIGSERIAL PRIMARY KEY,
    table_schema   TEXT NOT NULL,
    table_name     TEXT NOT NULL,
    column_name    TEXT,
    check_type     TEXT NOT NULL,   -- ej. not_null, format, range, unique, fk_integrity
    check_expr     TEXT,            -- expresión o regex usada para el control
    executed_at    TIMESTAMP WITH TIME ZONE DEFAULT now(),
    summary        JSONB            -- resumen cuantitativo del resultado (violaciones, totales, pct)
);

-- DQM: perfilado por tabla
CREATE TABLE IF NOT EXISTS dqm_table_profile (
    profile_id     BIGSERIAL PRIMARY KEY,
    table_schema   TEXT NOT NULL,
    table_name     TEXT NOT NULL,
    row_count      BIGINT,
    column_stats   JSONB,           -- { "col": {"nulls":.., "distinct":.., "min":.., "max":.., "pct_null":..}, ... }
    executed_at    TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- DQM: huella de procesos (transformaciones, copias, cargas)
CREATE TABLE IF NOT EXISTS dqm_process_footprint (
    footprint_id   BIGSERIAL PRIMARY KEY,
    process_name   TEXT NOT NULL,
    source         TEXT,
    target         TEXT,
    started_at     TIMESTAMP WITH TIME ZONE,
    finished_at    TIMESTAMP WITH TIME ZONE,
    rows_in        BIGINT,
    rows_out       BIGINT,
    status         TEXT,            -- 'RUNNING','OK','ERROR'
    metadata       JSONB
);