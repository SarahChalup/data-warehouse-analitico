-- =====================================================================
-- Creación de la Infraestructura DQM
-- =====================================================================
DO $$
DECLARE
    -- ... (Bloque de logging estándar del equipo) ...
    v_script_nombre TEXT := 'E2_05_dqm_create_infrastructure.sql';
    v_script_desc   TEXT := 'Crea la infraestructura del DQM: tablas de reglas/resultados. El procedimiento se crea por separado.';
    v_created_by    TEXT := 'Agustina';
    v_log_id BIGINT; v_script_id INT; v_error_msg TEXT; v_detail TEXT;
BEGIN
    -- 1. INICIO DEL LOG
    SELECT script_id INTO v_script_id FROM dqm_scripts_inventory WHERE script_name = v_script_nombre;
    IF v_script_id IS NULL THEN
        INSERT INTO dqm_scripts_inventory (script_name, description, created_by) VALUES (v_script_nombre, v_script_desc, v_created_by) RETURNING script_id INTO v_script_id;
    END IF;
    INSERT INTO dqm_exec_log (script_id, started_at, status) VALUES (v_script_id, NOW(), 'RUNNING') RETURNING log_id INTO v_log_id;

    -- 2. BLOQUE PRINCIPAL (Creación de Tablas)
    BEGIN
        -- PASO 1: CREACIÓN DE LAS TABLAS DEL DQM
        RAISE NOTICE 'Paso 1: Creando tablas dqm_reglas_calidad y dqm_resultados_calidad...';
        
        CREATE TABLE IF NOT EXISTS dqm_reglas_calidad (regla_id SERIAL PRIMARY KEY, nombre_regla VARCHAR(255) UNIQUE NOT NULL, descripcion TEXT, entidad_objetivo VARCHAR(100) NOT NULL, campo_objetivo VARCHAR(100), tipo_regla VARCHAR(50), umbral_error_porcentaje NUMERIC(5, 2) DEFAULT 0.00, query_sql_validacion TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS dqm_resultados_calidad (resultado_id SERIAL PRIMARY KEY, regla_id INTEGER NOT NULL REFERENCES dqm_reglas_calidad(regla_id), log_id INTEGER REFERENCES dqm_exec_log(log_id), fecha_ejecucion TIMESTAMP WITH TIME ZONE NOT NULL, total_registros_evaluados BIGINT, total_registros_fallidos BIGINT, porcentaje_fallo NUMERIC(5, 2), resultado_final VARCHAR(50));

        -- PASO 2: DOCUMENTACIÓN DEL DQM EN LA METADATA
        RAISE NOTICE 'Paso 2: Documentando la infraestructura DQM en la metadata...';
        INSERT INTO md_entities (entity_name, business_name, layer, entity_type, description, created_by) VALUES
        ('dqm_reglas_calidad', 'Catálogo de Reglas de Calidad', 'DQM', 'Catálogo', 'Catálogo central de reglas de calidad de datos.', v_created_by),
        ('dqm_resultados_calidad', 'Resultados de Validaciones', 'DQM', 'Log', 'Log histórico de resultados de validación de calidad.', v_created_by)
        ON CONFLICT (entity_name) DO UPDATE SET description = EXCLUDED.description;

        -- NO SE CREA EL PROCEDIMIENTO AQUÍ

        -- Actualizar el log para reflejar que esta parte fue exitosa
        UPDATE dqm_exec_log SET status = 'OK_PARTIAL', message = 'Tablas del DQM creadas y documentadas exitosamente.' WHERE log_id = v_log_id;
        RAISE NOTICE 'Script (Parte 1) % finalizado con ÉXITO.', v_script_nombre;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL;
            UPDATE dqm_exec_log SET finished_at = NOW(), status = 'CRITICAL_ERROR', message = 'Fallo en creación de infraestructura DQM: ' || v_error_msg WHERE log_id = v_log_id;
            RAISE WARNING 'Script % falló. Revisa dqm_exec_log ID %', v_script_nombre, v_log_id;
    END;
END $$;

-- =====================================================================
-- PASO 3: CREACIÓN DEL PROCEDIMIENTO (COMO COMANDO INDEPENDIENTE)
-- =====================================================================

CREATE OR REPLACE PROCEDURE ejecutar_chequeos_calidad(p_nombre_tabla TEXT, p_log_id INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    regla RECORD; v_total_filas BIGINT; v_filas_fallidas BIGINT; v_porc_fallo NUMERIC; v_resultado TEXT;
BEGIN
    FOR regla IN SELECT * FROM dqm_reglas_calidad WHERE entidad_objetivo = p_nombre_tabla LOOP
        EXECUTE format('SELECT COUNT(*) FROM %I', regla.entidad_objetivo) INTO v_total_filas;
        EXECUTE regla.query_sql_validacion INTO v_filas_fallidas;
        IF v_total_filas > 0 THEN v_porc_fallo := (v_filas_fallidas::NUMERIC / v_total_filas) * 100; ELSE v_porc_fallo := 0; END IF;
        IF v_porc_fallo > regla.umbral_error_porcentaje THEN v_resultado := 'Rechazado'; ELSE v_resultado := 'Aprobado'; END IF;
        INSERT INTO dqm_resultados_calidad (regla_id, log_id, fecha_ejecucion, total_registros_evaluados, total_registros_fallidos, porcentaje_fallo, resultado_final)
        VALUES (regla.regla_id, p_log_id, NOW(), v_total_filas, v_filas_fallidas, v_porc_fallo, v_resultado);
        RAISE NOTICE 'Regla "%" ejecutada. Resultado: %.', regla.nombre_regla, v_resultado;
    END LOOP;
END;
$$;

-- =====================================================================
-- PASO 4: ACTUALIZACIÓN FINAL DEL LOG
-- Para confirmar que todo el script (incluida la creación del procedimiento) se completó.
-- =====================================================================
DO $$
DECLARE
    v_log_id BIGINT;
    v_script_nombre TEXT := 'E2_07_dqm_create_infrastructure.sql';
BEGIN
    -- Buscamos el ID del log que iniciamos en la primera parte.
    SELECT MAX(l.log_id) INTO v_log_id 
    FROM dqm_exec_log l
    JOIN dqm_scripts_inventory s ON l.script_id = s.script_id
    WHERE s.script_name = v_script_nombre;

    -- Actualizamos el log para reflejar que el script completo finalizó con éxito.
    UPDATE dqm_exec_log SET
        finished_at = NOW(),
        status = 'OK',
        message = 'Infraestructura DQM completa (tablas y procedimiento) creada exitosamente.'
    WHERE log_id = v_log_id;
    RAISE NOTICE 'Script % finalizado completamente con ÉXITO.', v_script_nombre;
END $$;
