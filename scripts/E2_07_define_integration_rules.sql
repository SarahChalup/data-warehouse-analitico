-- =====================================================================
-- Definición de Controles de Calidad de Integración (Punto 8b)
-- =====================================================================
DO $$
DECLARE
    -- ... (Bloque de logging estándar del equipo) ...
    v_script_nombre TEXT := 'E2_07_define_integration_rules.sql';
    v_script_desc   TEXT := 'Define y persiste las reglas de calidad de Integración (Consistencia) en la tabla dqm_reglas_calidad.';
    v_created_by    TEXT := 'Agustina';
    v_log_id BIGINT; v_script_id INT; v_error_msg TEXT; v_detail TEXT;
BEGIN
    SELECT script_id INTO v_script_id FROM dqm_scripts_inventory WHERE script_name = v_script_nombre;
    IF v_script_id IS NULL THEN
        INSERT INTO dqm_scripts_inventory (script_name, description, created_by) VALUES (v_script_nombre, v_script_desc, v_created_by) RETURNING script_id INTO v_script_id;
    END IF;
    INSERT INTO dqm_exec_log (script_id, started_at, status) VALUES (v_script_id, NOW(), 'RUNNING') RETURNING log_id INTO v_log_id;

    BEGIN
        RAISE NOTICE 'Definiendo reglas de calidad de Integración (Consistencia)...';

        INSERT INTO dqm_reglas_calidad (nombre_regla, descripcion, entidad_objetivo, campo_objetivo, tipo_regla, umbral_error_porcentaje, query_sql_validacion) VALUES
        -- Reglas de CONSISTENCIA
        ('CONSISTENCIA_FK_PRODUCTO', 'Cada product_key en fact_table debe existir en dim_product.', 'fact_table', 'product_key', 'Consistencia', 0.00, 'SELECT COUNT(*) FROM fact_table f LEFT JOIN dim_product d ON f.product_key = d.product_key WHERE d.product_key IS NULL AND f.product_key IS NOT NULL'),
        ('CONSISTENCIA_FK_CLIENTE', 'Cada customer_key en fact_table debe existir en dim_customer.', 'fact_table', 'customer_key', 'Consistencia', 0.00, 'SELECT COUNT(*) FROM fact_table f LEFT JOIN dim_customer d ON f.customer_key = d.customer_key WHERE d.customer_key IS NULL AND f.customer_key IS NOT NULL'),
        ('CONSISTENCIA_FK_EMPLEADO', 'Cada employee_key en fact_table debe existir en dim_employee.', 'fact_table', 'employee_key', 'Consistencia', 0.00, 'SELECT COUNT(*) FROM fact_table f LEFT JOIN dim_employee d ON f.employee_key = d.employee_key WHERE d.employee_key IS NULL AND f.employee_key IS NOT NULL'),
        ('CONSISTENCIA_FK_PROD_CAT', 'Cada category_key en dim_product debe existir en dim_category.', 'dim_product', 'category_key', 'Consistencia', 0.00, 'SELECT COUNT(*) FROM dim_product p LEFT JOIN dim_category c ON p.category_key = c.category_key WHERE c.category_key IS NULL AND p.category_key IS NOT NULL')
        ON CONFLICT (nombre_regla) DO UPDATE SET 
            descripcion = EXCLUDED.descripcion,
            query_sql_validacion = EXCLUDED.query_sql_validacion,
            umbral_error_porcentaje = EXCLUDED.umbral_error_porcentaje;

        UPDATE dqm_exec_log SET finished_at = NOW(), status = 'OK', message = 'Reglas de Integración definidas exitosamente.' WHERE log_id = v_log_id;
        RAISE NOTICE 'Script % finalizado con ÉXITO.', v_script_nombre;
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL;
            UPDATE dqm_exec_log SET finished_at = NOW(), status = 'CRITICAL_ERROR', message = 'Fallo al definir reglas de Integración: ' || v_error_msg WHERE log_id = v_log_id;
            RAISE WARNING 'Script % falló. Revisa dqm_exec_log ID %', v_script_nombre, v_log_id;
    END;
END $$;