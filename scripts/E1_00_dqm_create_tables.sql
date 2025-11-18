-- Inventario de scripts (control)
CREATE TABLE IF NOT EXISTS dqm_scripts_inventory (
    script_id    SERIAL PRIMARY KEY,
    script_name  TEXT NOT NULL,
    description  TEXT,
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

CREATE TABLE dqm_object_inventory (
    object_id      SERIAL PRIMARY KEY,
    object_name    TEXT NOT NULL,       -- nombre exacto del objeto creado
    object_type    TEXT NOT NULL,       -- table, view, sequence, function
    created_by_script INT REFERENCES dqm_scripts_inventory(script_id),
    created_at     TIMESTAMP DEFAULT NOW()
);

-- DQM: checks por campo
CREATE TABLE IF NOT EXISTS dqm_field_checks (
    check_id       BIGSERIAL PRIMARY KEY,
    table_name     TEXT NOT NULL,
    column_name    TEXT,
    rule_type      TEXT NOT NULL,       -- NOT_NULL, RANGE, DOMAIN, REGEX, TYPE, UNIQUE
    rule_param     TEXT,                -- regex, lista de valores, rango, tipo esperado, etc.
    active         BOOLEAN DEFAULT TRUE,
    description    TEXT,
    created_at     TIMESTAMP DEFAULT NOW()         -- resumen cuantitativo del resultado (violaciones, totales, pct)
);

-- DQM: perfilado por tabla
CREATE TABLE IF NOT EXISTS dqm_table_profile (
    profile_id     BIGSERIAL PRIMARY KEY,
    table_name     TEXT NOT NULL,
    column_name    TEXT NOT NULL,
    row_count      BIGINT,
    null_count     BIGINT,
    distinct_count BIGINT,
    min_value      TEXT,
    max_value      TEXT,
    avg_length     NUMERIC,
    max_length     INT,
    detected_type  TEXT,        -- { "col": {"nulls":.., "distinct":.., "min":.., "max":.., "pct_null":..}, ... }
    executed_at    TIMESTAMP WITH TIME ZONE DEFAULT now()
);

