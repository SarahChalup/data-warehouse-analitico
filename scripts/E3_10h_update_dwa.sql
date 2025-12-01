-- =====================================================================
-- SCRIPT E3_10h: Orquestador de Actualización del DWA con Deltas (Ingesta2)
-- =====================================================================
DO $$
DECLARE
    v_script_nombre TEXT := 'E3_10h_update_dwa_delta.sql';
    v_script_desc   TEXT := 'Actualiza el DWA con Ingesta2 (deltas): aplica SCD2 para modificaciones, inserta altas y valida con DQM.';
    v_created_by    TEXT := 'Equipo DWH';
    v_log_id BIGINT; v_script_id INT; v_error_msg TEXT; v_detail TEXT;
    v_errores_criticos INT;
BEGIN
    SELECT script_id INTO v_script_id FROM dqm_scripts_inventory WHERE script_name = v_script_nombre;
    IF v_script_id IS NULL THEN INSERT INTO dqm_scripts_inventory (script_name, description, created_by) VALUES (v_script_nombre, v_script_desc, v_created_by) RETURNING script_id INTO v_script_id; END IF;
    INSERT INTO dqm_exec_log (script_id, started_at, status) VALUES (v_script_id, NOW(), 'RUNNING') RETURNING log_id INTO v_log_id;

    BEGIN
        -- (PUNTO c) DETECTAR CAMBIOS (Lógica Simplificada)
        RAISE NOTICE 'Detectando altas y modificaciones en dim_customer...';
        CREATE TEMP TABLE temp_customer_changes AS
        SELECT
            t.customer_id,
            CASE WHEN d.customer_key IS NULL THEN 'NUEVO' ELSE 'MODIFICADO' END as change_status
        FROM tmp_customers t
        LEFT JOIN dim_customer d ON t.customer_id = d.customer_id AND d.es_actual = TRUE;

        -- (PUNTO f) APLICAR CAMBIOS (Capa de Memoria)
        
        -- Procesar Modificaciones (SCD Tipo 2)
        RAISE NOTICE 'Aplicando modificaciones (SCD Tipo 2) a dim_customer...';
        -- 1. Expirar los registros antiguos que van a ser modificados
        UPDATE dim_customer d SET es_actual = FALSE, fecha_hasta = NOW()
        FROM temp_customer_changes tc WHERE d.customer_id = tc.customer_id AND tc.change_status = 'MODIFICADO' AND d.es_actual = TRUE;
        -- 2. Insertar las nuevas versiones de los registros modificados
        INSERT INTO dim_customer (customer_id, company_name, city, country, region, fecha_desde, fecha_hasta, es_actual)
        SELECT t.customer_id, t.company_name, t.city, t.country, t.region, NOW(), NULL, TRUE
        FROM tmp_customers t JOIN temp_customer_changes tc ON t.customer_id = tc.customer_id AND tc.change_status = 'MODIFICADO';
        
        -- Procesar Altas (Nuevos registros)
        RAISE NOTICE 'Insertando nuevos clientes...';
        INSERT INTO dim_customer (customer_id, company_name, city, country, region, fecha_desde, fecha_hasta, es_actual)
        SELECT t.customer_id, t.company_name, t.city, t.country, t.region, NOW(), NULL, TRUE
        FROM tmp_customers t JOIN temp_customer_changes tc ON t.customer_id = tc.customer_id AND tc.change_status = 'NUEVO';
        
        -- (PUNTO h y g) CARGAR NUEVOS HECHOS (Capa de Enriquecimiento)
        RAISE NOTICE 'Cargando nuevos hechos en fact_table...';
        -- La lógica aquí no cambia, ya que solo procesa lo que hay en tmp_order_details
        INSERT INTO fact_table (product_key, customer_key, employee_key, date_key, quantity, unit_price, discount, total_amount)
        SELECT 
            dp.product_key, dc.customer_key, de.employee_key, TO_CHAR(o.order_date::date, 'YYYYMMDD')::INT,
            od.quantity::integer, od.unit_price::numeric, od.discount::numeric,
            (od.quantity::integer * od.unit_price::numeric * (1 - od.discount::numeric)) -- Enriquecimiento
        FROM tmp_order_details od
        JOIN tmp_orders o ON od.order_id = o.order_id
        LEFT JOIN dim_product dp ON od.product_id::integer = dp.product_id
        LEFT JOIN dim_customer dc ON o.customer_id = dc.customer_id AND dc.es_actual = TRUE -- Siempre unir con el registro ACTUAL
        LEFT JOIN dim_employee de ON o.employee_id::integer = de.employee_id;

        -- (PUNTO h, i) VERIFICAR REGLAS Y ACTUALIZAR DQM
        RAISE NOTICE 'Ejecutando validaciones DQM post-actualización...';
        CALL ejecutar_chequeos_calidad('dim_customer', v_log_id);
        CALL ejecutar_chequeos_calidad('fact_table', v_log_id);

        -- (PUNTO d) DECISIÓN FINAL
        SELECT COUNT(*) INTO v_errores_criticos
        FROM dqm_resultados_calidad r JOIN dqm_reglas_calidad q ON r.regla_id = q.regla_id
        WHERE r.log_id = v_log_id AND r.resultado_final = 'Rechazado' AND q.umbral_error_porcentaje = 0.00;

        IF v_errores_criticos > 0 THEN
            RAISE EXCEPTION 'Se encontraron % errores críticos de DQM. Revirtiendo la actualización...', v_errores_criticos;
        END IF;
        
        UPDATE dqm_exec_log SET finished_at = NOW(), status = 'OK', message = 'Actualización del DWA con deltas (Ingesta2) completada y validada.' WHERE log_id = v_log_id;
        RAISE NOTICE 'Script % finalizado con ÉXITO.', v_script_nombre;
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL;
            UPDATE dqm_exec_log SET finished_at = NOW(), status = 'CRITICAL_ERROR', message = 'Fallo en la actualización del DWA: ' || v_error_msg WHERE log_id = v_log_id;
            RAISE WARNING 'Script % falló. La transacción ha sido revertida. Revisa dqm_exec_log ID %', v_script_nombre, v_log_id;
    END;
END $$;