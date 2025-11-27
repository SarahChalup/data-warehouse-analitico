-- =====================================================================
-- E2_08c_PART2: CARGA DEL MODELO DIMENSIONAL (DWA)
-- Objetivo: Poblar el Snowflake Schema (Dim y Fact) desde Memoria Enriquecida.
-- =====================================================================

DO $$
DECLARE
    -- === Bloque de Logging ===
    v_script_nombre TEXT := 'E2_10_dwa_insert_data.sql';
    v_script_desc   TEXT := 'Carga del modelo Snowflake (DWA) utilizando vistas ENR.';
    v_created_by    TEXT := 'Data Engineer';
    v_log_id BIGINT; v_script_id INT; v_error_msg TEXT;
        v_msg text;
    v_detail text;
    v_hint text;
    v_context text;
BEGIN
    -- 1. INICIO DEL LOG
    SELECT script_id INTO v_script_id FROM dqm_scripts_inventory WHERE script_name = v_script_nombre;
    IF v_script_id IS NULL THEN
        INSERT INTO dqm_scripts_inventory (script_name, description, created_by) VALUES (v_script_nombre, v_script_desc, v_created_by) RETURNING script_id INTO v_script_id;
    END IF;
    INSERT INTO dqm_exec_log (script_id, started_at, status) VALUES (v_script_id, NOW(), 'RUNNING') RETURNING log_id INTO v_log_id;

    RAISE NOTICE '--- INICIO FASE 2: CARGA DEL DWA (SNOWFLAKE) ---';

    BEGIN
    -- ==========================================================

    -- 2.1 TIEMPO (Generación automática)
    TRUNCATE TABLE dim_time CASCADE;
    INSERT INTO dim_time (date_key, full_date, year, month, quarter, day, is_weekend)
    SELECT TO_CHAR(datum, 'YYYYMMDD')::INTEGER, datum, EXTRACT(YEAR FROM datum), EXTRACT(MONTH FROM datum), 'Q' || EXTRACT(QUARTER FROM datum), EXTRACT(DAY FROM datum), CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END
    FROM (SELECT '1996-01-01'::DATE + sequence.day AS datum FROM generate_series(0, 15000) AS sequence(day)) DQ;
    RAISE NOTICE 'Dimensión Tiempo generada.';

    -- 2.2 DIMENSIONES INDEPENDIENTES (Padres)
    -- Leemos desde las vistas ENR que creamos antes
    TRUNCATE TABLE dim_category CASCADE;
    INSERT INTO dim_category (category_id, category_name, load_date) 
    SELECT category_id, category_name, NOW() FROM enr_dim_categories;

    TRUNCATE TABLE dim_supplier CASCADE;
    INSERT INTO dim_supplier (supplier_id, company_name, load_date) 
    SELECT supplier_id, company_name, NOW() FROM enr_dim_suppliers;

    TRUNCATE TABLE dim_customer CASCADE;
    INSERT INTO dim_customer (customer_id, company_name, city, country, region, load_date)
    SELECT customer_id, company_name, city, country, region, NOW() FROM enr_dim_customers;

    TRUNCATE TABLE dim_employee CASCADE;
    INSERT INTO dim_employee (employee_id, full_name, title, city, country, region, hire_date, load_date)
    SELECT employee_id, full_name, title, city, country, region, hire_date, NOW() FROM enr_dim_employees;
    
    RAISE NOTICE 'Dimensiones independientes cargadas.';

    -- 2.3 DIMENSION PRODUCTO (Dependiente - Snowflake)
    -- Requiere cruzar con dim_category y dim_supplier para obtener las KEYS subrogadas
    TRUNCATE TABLE dim_product CASCADE;
    INSERT INTO dim_product (product_id, product_name, category_key, supplier_key, discontinued, load_date)
    SELECT 
        p.product_id, 
        p.product_name, 
        c.category_key, 
        s.supplier_key, 
        p.is_discontinued, -- Viene ya convertido a Boolean desde la vista ENR
        NOW()
    FROM enr_dim_products p
    LEFT JOIN dim_category c ON p.category_id = c.category_id
    LEFT JOIN dim_supplier s ON p.supplier_id = s.supplier_id;
    RAISE NOTICE 'Dimensión Producto cargada (Copo de Nieve).';

    -- 2.4 TABLA DE HECHOS
    -- Cruce final para obtener todas las llaves foráneas
    TRUNCATE TABLE fact_table CASCADE;
    INSERT INTO fact_table (product_key, customer_key, employee_key, date_key, quantity, unit_price, discount, total_amount, load_date)
    SELECT 
        dp.product_key, 
        dc.customer_key, 
        de.employee_key, 
        dt.date_key, 
        f.quantity, 
        f.unit_price, 
        f.discount, 
        f.total_amount, -- Calculado en la vista ENR
        NOW()
    FROM enr_fact_sales f
    JOIN dim_product dp ON f.product_id = dp.product_id
    JOIN dim_customer dc ON f.customer_id = dc.customer_id
    JOIN dim_employee de ON f.employee_id = de.employee_id
    JOIN dim_time dt ON f.date_key = dt.date_key;
    
    RAISE NOTICE 'Tabla de Hechos cargada exitosamente.';

      UPDATE dqm_exec_log
        SET finished_at = NOW(),
            status = 'OK',
            message = 'Completado exitosamente.',
            rows_processed = 0 -- El valor acumulado
        WHERE log_id = v_log_id;

     RAISE NOTICE 'Script finalizado exitosamente sin errores.';

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT;
         -- Actualizamos log a ERROR CRITICO
            UPDATE dqm_exec_log
            SET finished_at = NOW(),
                status = 'CRITICAL_ERROR', -- Diferente a error de datos
                message = 'Fallo Técnico: ' || v_msg || ' Detalle: ' || v_detail
            WHERE log_id = v_log_id;

            -- IMPORTANTE: NO hacemos RAISE EXCEPTION aquí.
            -- Hacemos RAISE NOTICE para que el script termine "bien" a ojos de SQL
            -- y se guarde el INSERT/UPDATE del log.
            RAISE NOTICE 'El script falló técnicamente. Revisa dqm_exec_log ID %', v_log_id;
    
    END;
END $$;