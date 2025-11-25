-- =====================================================================
-- Auditoría de Controles de Calidad de INGESTA
-- =====================================================================
DO $$
DECLARE
    -- === Bloque de Logging ===
    v_script_nombre TEXT := 'E2_06_audit_ingestion_rules.sql';
    v_script_desc   TEXT := 'Audita y muestra las reglas de calidad de Ingesta (Validez, Completitud) definidas en el DQM.';
    v_created_by    TEXT := 'Agustina';
    v_log_id BIGINT; v_script_id INT; v_error_msg TEXT; v_detail TEXT;

    -- === Variable para el bucle ===
    regla RECORD;
    
BEGIN
    -- Inicio del Logging
    SELECT script_id INTO v_script_id FROM dqm_scripts_inventory WHERE script_name = v_script_nombre;
    IF v_script_id IS NULL THEN
        INSERT INTO dqm_scripts_inventory (script_name, description, created_by) VALUES (v_script_nombre, v_script_desc, v_created_by) RETURNING script_id INTO v_script_id;
    END IF;
    INSERT INTO dqm_exec_log (script_id, started_at, status) VALUES (v_script_id, NOW(), 'RUNNING') RETURNING log_id INTO v_log_id;

    BEGIN
        RAISE NOTICE '--- INICIO REPORTE DE AUDITORÍA: REGLAS DE INGESTA (PUNTO 8a) ---';
        RAISE NOTICE '------------------------------------------------------------------';
        RAISE NOTICE '';

        FOR regla IN 
            SELECT * FROM dqm_reglas_calidad 
            WHERE tipo_regla IN ('Validez', 'Completitud') 
            ORDER BY tipo_regla, nombre_regla 
        LOOP
            RAISE NOTICE 'Regla: %, Tipo: %, Tabla: %, Límite de Error: % %%', regla.nombre_regla, regla.tipo_regla, regla.entidad_objetivo, regla.umbral_error_porcentaje;
            RAISE NOTICE '   Descripción: %', regla.descripcion;
            RAISE NOTICE '';
        END LOOP;

        RAISE NOTICE '------------------------------------------------------------------';
        RAISE NOTICE '--- FIN REPORTE DE AUDITORÍA ---';

        UPDATE dqm_exec_log SET finished_at = NOW(), status = 'OK', message = 'Auditoría de reglas de Ingesta completada exitosamente.' WHERE log_id = v_log_id;
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL;
            UPDATE dqm_exec_log SET finished_at = NOW(), status = 'CRITICAL_ERROR', message = 'Fallo en la auditoría: ' || v_error_msg WHERE log_id = v_log_id;
            RAISE WARNING 'El script de auditoría de Ingesta falló. Revisa dqm_exec_log ID %', v_log_id;
    END;
END $$;