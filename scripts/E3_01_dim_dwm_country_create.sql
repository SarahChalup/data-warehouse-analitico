---------------------------------------------------------------
-- E3_09_dim_country_create.sql
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
        'E3_01_dim_country_create.sql',
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
    
        CREATE TABLE txt_countries (
            country_name TEXT,
            density_per_km2 TEXT,
            abbreviation TEXT,
            agricultural_land_percent TEXT,
            land_area_km2 TEXT,
            armed_forces_size TEXT,
            birth_rate TEXT,
            calling_code TEXT,
            capital_city TEXT,
            co2_emissions TEXT,
            cpi TEXT,
            cpi_change_percent TEXT,
            currency_code TEXT,
            fertility_rate TEXT,
            forested_area_percent TEXT,
            gasoline_price TEXT,
            gdp TEXT,
            gross_primary_education_enrollment_percent TEXT,
            gross_tertiary_education_enrollment_percent TEXT,
            infant_mortality TEXT,
            largest_city TEXT,
            life_expectancy TEXT,
            maternal_mortality_ratio TEXT,
            minimum_wage TEXT,
            official_language TEXT,
            out_of_pocket_health_expenditure_percent TEXT,
            physicians_per_thousand TEXT,
            population TEXT,
            labor_force_participation_percent TEXT,
            tax_revenue_percent TEXT,
            total_tax_rate_percent TEXT,
            unemployment_rate_percent TEXT,
            urban_population TEXT,
            latitude TEXT,
            longitude TEXT
        );

        CREATE TABLE tmp_countries (
            id SERIAL PRIMARY KEY, -- Agregué un ID autoincremental como buena práctica
            country_name TEXT,
            density_per_km2 NUMERIC,
            abbreviation VARCHAR(5),
            agricultural_land_percent NUMERIC,
            land_area_km2 NUMERIC,
            armed_forces_size INTEGER,
            birth_rate NUMERIC,
            calling_code VARCHAR(10), -- VARCHAR es mejor por si incluye símbolos como '+'
            capital_city TEXT,
            co2_emissions NUMERIC,
            cpi NUMERIC,
            cpi_change_percent NUMERIC,
            currency_code VARCHAR(10),
            fertility_rate NUMERIC,
            forested_area_percent NUMERIC,
            gasoline_price NUMERIC,
            gdp NUMERIC, -- NUMERIC o BIGINT son ideales para cifras monetarias grandes
            gross_primary_education_enrollment_percent NUMERIC,
            gross_tertiary_education_enrollment_percent NUMERIC,
            infant_mortality NUMERIC,
            largest_city TEXT,
            life_expectancy NUMERIC,
            maternal_mortality_ratio INTEGER,
            minimum_wage NUMERIC,
            official_language TEXT,
            out_of_pocket_health_expenditure_percent NUMERIC,
            physicians_per_thousand NUMERIC,
            population BIGINT,
            labor_force_participation_percent NUMERIC,
            tax_revenue_percent NUMERIC,
            total_tax_rate_percent NUMERIC,
            unemployment_rate_percent NUMERIC,
            urban_population BIGINT,
            latitude NUMERIC,
            longitude NUMERIC
        );


        CREATE TABLE dwm_countries (
            id SERIAL PRIMARY KEY, -- Agregué un ID autoincremental como buena práctica
            country_name TEXT,
            density_per_km2 NUMERIC,
            abbreviation VARCHAR(5),
            agricultural_land_percent NUMERIC,
            land_area_km2 NUMERIC,
            armed_forces_size INTEGER,
            birth_rate NUMERIC,
            calling_code VARCHAR(10), -- VARCHAR es mejor por si incluye símbolos como '+'
            capital_city TEXT,
            co2_emissions NUMERIC,
            cpi NUMERIC,
            cpi_change_percent NUMERIC,
            currency_code VARCHAR(10),
            fertility_rate NUMERIC,
            forested_area_percent NUMERIC,
            gasoline_price NUMERIC,
            gdp NUMERIC, -- NUMERIC o BIGINT son ideales para cifras monetarias grandes
            gross_primary_education_enrollment_percent NUMERIC,
            gross_tertiary_education_enrollment_percent NUMERIC,
            infant_mortality NUMERIC,
            largest_city TEXT,
            life_expectancy NUMERIC,
            maternal_mortality_ratio INTEGER,
            minimum_wage NUMERIC,
            official_language TEXT,
            out_of_pocket_health_expenditure_percent NUMERIC,
            physicians_per_thousand NUMERIC,
            population BIGINT,
            labor_force_participation_percent NUMERIC,
            tax_revenue_percent NUMERIC,
            total_tax_rate_percent NUMERIC,
            unemployment_rate_percent NUMERIC,
            urban_population BIGINT,
            latitude NUMERIC,
            longitude NUMERIC,
            load_date        TIMESTAMP DEFAULT NOW(), -- Auditoría
            active           BOOLEAN DEFAULT TRUE  -- Auditoría
        );


    CREATE TABLE IF NOT EXISTS dim_country (
        country_key      SERIAL PRIMARY KEY,    -- Surrogate Key (Interna del DW)
        country_name     VARCHAR(100),          -- Business Key (Nombre para JOINS)
        abbreviation     VARCHAR(5),            -- Código ISO (ej: AR, US)

        -- 2. JERARQUÍA GEOGRÁFICA
        capital_city     VARCHAR(100),          -- Para drill-down (País -> Capital)
        latitude         NUMERIC(10, 6),        -- Indispensable para Mapas en PowerBI
        longitude        NUMERIC(10, 6),        -- Indispensable para Mapas en PowerBI

        -- 3. DATOS DE MERCADO (Para análisis de potencial de ventas)
        population       BIGINT,                -- Para calcular "Ventas per Cápita"
        gdp              NUMERIC(18, 2),        -- PBI: Para medir poder adquisitivo del mercado
        currency_code    VARCHAR(10),           -- Útil para conversiones monetarias futuras
        official_language VARCHAR(50),          -- Útil para segmentación de marketing (idioma)

        -- 4. INDICADORES ECONÓMICOS (Contexto de ventas)
        unemployment_rate NUMERIC(5, 2),        -- Puede explicar caídas en ventas en una región
        cpi               NUMERIC(10, 2)       -- Índice precios al consumidor (Inflación)
    );


-- 1. Modificar Dimension Cliente
ALTER TABLE dim_customer 
ADD COLUMN country_key INTEGER REFERENCES dim_country(country_key);

-- 2. Modificar Dimension Proveedor
ALTER TABLE dim_supplier 
ADD COLUMN country_key INTEGER REFERENCES dim_country(country_key);

-- 3. Modificar Dimension Empleado
ALTER TABLE dim_employee 
ADD COLUMN country_key INTEGER REFERENCES dim_country(country_key);




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
