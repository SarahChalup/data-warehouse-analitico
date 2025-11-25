-- =====================================================================
-- Diseño y Creación del DQM
-- =====================================================================
DO $$
DECLARE
    v_log_id BIGINT; v_script_id INT; v_msg TEXT; v_detail TEXT;
    v_script_name TEXT := 'E2_07_dqm_create_system.sql';
BEGIN
    -- 1. SETUP E INICIO DEL LOGGING
    SELECT script_id INTO v_script_id FROM dqm_scripts_inventory WHERE script_name = v_script_name;
    IF v_script_id IS NULL THEN
        INSERT INTO dqm_scripts_inventory (script_name, description, created_by) VALUES (v_script_name, 'Crea el DQM Operacional: tablas, reglas y procedimiento.', 'Agus') RETURNING script_id INTO v_script_id;
    END IF;
    INSERT INTO dqm_exec_log (script_id, started_at, status) VALUES (v_script_id, NOW(), 'RUNNING') RETURNING log_id INTO v_log_id;

    -- 2. BLOQUE PRINCIPAL (Creación de Tablas y Reglas)
    BEGIN
        -- PASO 2.1: CREACIÓN DEL DQM OPERACIONAL EN 'public'
        RAISE NOTICE 'Paso 2.1: Creando tablas del DQM en el esquema public...';
        
        CREATE TABLE IF NOT EXISTS dqm_reglas_calidad (regla_id SERIAL PRIMARY KEY, nombre_regla VARCHAR(255) UNIQUE NOT NULL, descripcion TEXT, entidad_objetivo VARCHAR(100) NOT NULL, campo_objetivo VARCHAR(100), tipo_regla VARCHAR(50), umbral_error_porcentaje NUMERIC(5, 2) DEFAULT 0.00, query_sql_validacion TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS dqm_resultados_calidad (resultado_id SERIAL PRIMARY KEY, regla_id INTEGER NOT NULL REFERENCES dqm_reglas_calidad(regla_id), log_id INTEGER REFERENCES dqm_exec_log(log_id), fecha_ejecucion TIMESTAMP WITH TIME ZONE NOT NULL, total_registros_evaluados BIGINT, total_registros_fallidos BIGINT, porcentaje_fallo NUMERIC(5, 2), resultado_final VARCHAR(50));

        -- PASO 2.2: ASEGURAR RESTRICCIONES UNIQUE EN METADATA
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_md_entities_entity_name' AND conrelid = 'md_entities'::regclass) THEN ALTER TABLE md_entities ADD CONSTRAINT uq_md_entities_entity_name UNIQUE (entity_name); END IF;
        
        -- PASO 2.3: DEFINICIÓN DE REGLAS DE CALIDAD
        RAISE NOTICE 'Paso 2.3: Definiendo las reglas de calidad...';
        
      INSERT INTO dqm_reglas_calidad (nombre_regla, descripcion, entidad_objetivo, campo_objetivo, tipo_regla, umbral_error_porcentaje, query_sql_validacion) VALUES
-- Reglas de Validez
('VENTA_PRECIO_NO_NEGATIVO', 'El precio unitario no puede ser menor a cero.', 'fact_table', 'unit_price', 'Validez', 0.00,
 'SELECT COUNT(*) FROM fact_table WHERE unit_price < 0'),
('VENTA_CANTIDAD_NO_NEGATIVA', 'La cantidad vendida no puede ser cero o negativa.', 'fact_table', 'quantity', 'Validez', 0.00,
 'SELECT COUNT(*) FROM fact_table WHERE quantity <= 0'),
('VENTA_TOTAL_CONSISTENTE', 'El monto total derivado debe ser consistente con precio, cantidad y descuento.', 'fact_table', 'total_amount', 'Validez', 1.00,
 'SELECT COUNT(*) FROM fact_table WHERE total_amount <> (unit_price * quantity * (1 - discount))'),

-- Reglas de Completitud
('VENTA_FK_CLIENTE_COMPLETA', 'Todas las ventas deben estar asociadas a un cliente válido.', 'fact_table', 'customer_key', 'Completitud', 1.00,
 'SELECT COUNT(*) FROM fact_table WHERE customer_key IS NULL'),
('CLIENTE_NOMBRE_COMPLETO', 'El nombre de la compañía del cliente no puede ser nulo o vacío.', 'dim_customer', 'company_name', 'Completitud', 2.00,
 'SELECT COUNT(*) FROM dim_customer WHERE company_name IS NULL OR company_name = '''''),

-- Reglas de Consistencia
('CONSISTENCIA_FK_PRODUCTO', 'Cada product_key en la tabla de hechos debe existir en la dimensión de productos.', 'fact_table', 'product_key', 'Consistencia', 0.00,
 'SELECT COUNT(*) FROM fact_table f LEFT JOIN dim_product d ON f.product_key = d.product_key WHERE d.product_key IS NULL AND f.product_key IS NOT NULL'),
('CONSISTENCIA_FK_CLIENTE', 'Cada customer_key en la tabla de hechos debe existir en la dimensión de clientes.', 'fact_table', 'customer_key', 'Consistencia', 0.00,
 'SELECT COUNT(*) FROM fact_table f LEFT JOIN dim_customer d ON f.customer_key = d.customer_key WHERE d.customer_key IS NULL AND f.customer_key IS NOT NULL')

ON CONFLICT (nombre_regla) DO NOTHING;
        UPDATE dqm_exec_log SET status = 'OK_PARTIAL', message = 'Tablas y reglas del DQM creadas exitosamente.' WHERE log_id = v_log_id;
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL;
            UPDATE dqm_exec_log SET finished_at = NOW(), status = 'CRITICAL_ERROR', message = 'Fallo en creación de tablas/reglas: ' || v_msg || ' | ' || v_detail WHERE log_id = v_log_id;
            RAISE WARNING 'El script % falló en la Parte 1. Revisa dqm_exec_log ID %', v_script_name, v_log_id;
            RETURN;
    END;
END $$;

-- =====================================================================
-- PASO 3: CREACIÓN DEL PROCEDIMIENTO (FUERA DEL BLOQUE DO)
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
        RAISE NOTICE 'Regla "%" ejecutada. Resultado: %. Fallos: % de % (% %%)', regla.nombre_regla, v_resultado, v_filas_fallidas, v_total_filas, round(v_porc_fallo, 2);
    END LOOP;
END;
$$;

-- =====================================================================
-- PASO 4: ACTUALIZACIÓN FINAL DEL LOG
-- =====================================================================
DO $$
DECLARE
    v_log_id BIGINT; v_script_name TEXT := 'E2_07_dqm_create_system.sql';
BEGIN
    SELECT MAX(log_id) INTO v_log_id FROM dqm_exec_log l
    JOIN dqm_scripts_inventory s ON l.script_id = s.script_id
    WHERE s.script_name = v_script_name;

    UPDATE dqm_exec_log SET
        finished_at = NOW(),
        status = 'OK',
        message = 'Sistema DQM completo (tablas, reglas y procedimiento) creado exitosamente.'
    WHERE log_id = v_log_id;
END $$;
