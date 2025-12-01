---------------------------------------------------------------
-- E3_01_dim_country_create.sql
-- Crea la dimensión de países (dim_country) basada en World-Data-2023
-- y registra la ejecución en el DQM.
-- Etapa 3 – Actualización
-- Autora: Mariana
---------------------------------------------------------------

DO $$
DECLARE
    v_log_id     BIGINT;
    v_script_id  INT;
BEGIN
    -------------------------------------------------------------------
    -- 1. Registrar este script en el inventario DQM 
    -------------------------------------------------------------------
    INSERT INTO dqm_scripts_inventory (
        script_name,
        description,
        created_by,
        created_at
    )
    VALUES (
        'E3_09_dim_country_create.sql',
        'Crea la dimensión dim_country para integrar datos de países (World-Data-2023).',
        'Mariana',
        NOW()
    )
    RETURNING script_id INTO v_script_id;

    -------------------------------------------------------------------
    -- 2. Registrar inicio de ejecución en dqm_exec_log
    -------------------------------------------------------------------
    INSERT INTO dqm_exec_log (script_id, started_at, status)
    VALUES (v_script_id, NOW(), 'RUNNING')
    RETURNING log_id INTO v_log_id;

    -------------------------------------------------------------------
    -- 3. CREACIÓN DE LA DIMENSIÓN DE PAÍSES EN EL DWA
    -------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS dim_country (
        country_id      SERIAL PRIMARY KEY,         -- surrogate key
        country_name    VARCHAR(200) NOT NULL,      -- nombre del país
        country_code    VARCHAR(10),                -- ISO u otro código
        region          VARCHAR(200),               -- región (Europa, América…)
        subregion       VARCHAR(200),               -- subregión geográfica
        income_group    VARCHAR(200),               -- grupo de ingresos si existe
        population_2023 BIGINT,                     -- población según dataset
        source_system   VARCHAR(50) DEFAULT 'World-Data-2023',
        valid_from      DATE DEFAULT CURRENT_DATE,
        valid_to        DATE,
        is_current      BOOLEAN DEFAULT TRUE
    );

    -------------------------------------------------------------------
    -- 4. Registrar el objeto creado en dqm_object_inventory
    -------------------------------------------------------------------
    INSERT INTO dqm_object_inventory (
        object_name,
        object_type,
        created_by_script,
        created_at
    )
    VALUES (
        'dim_country',
        'table',
        v_script_id,
        NOW()
    )
    ON CONFLICT DO NOTHING;

    -------------------------------------------------------------------
    -- 5. Cerrar el log de ejecución en DQM
    -------------------------------------------------------------------
    UPDATE dqm_exec_log
    SET finished_at   = NOW(),
        status        = 'OK',
        message       = 'dim_country creada correctamente como parte de E3_09.',
        rows_processed = 0
    WHERE log_id = v_log_id;

END $$;
