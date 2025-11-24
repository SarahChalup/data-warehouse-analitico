-- =====================================================================
-- DISEÑO, CREACIÓN Y USO DEL DQM
-- =====================================================================

-- =====================================================================
-- PASO 1: CREACIÓN DEL ESQUEMA Y LAS TABLAS DEL DQM
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS DQM;
CREATE TABLE IF NOT EXISTS DQM.log_procesos (log_id SERIAL PRIMARY KEY, proceso_nombre VARCHAR(255) NOT NULL, fecha_inicio TIMESTAMP WITH TIME ZONE NOT NULL, fecha_fin TIMESTAMP WITH TIME ZONE, estado VARCHAR(50) NOT NULL CHECK (estado IN ('Exitoso', 'Fallido', 'En Ejecucion')), registros_afectados INTEGER, mensaje TEXT);
CREATE TABLE IF NOT EXISTS DQM.perfilado_entidades (perfilado_id SERIAL PRIMARY KEY, log_proceso_id INTEGER REFERENCES DQM.log_procesos(log_id), fecha_perfilado DATE NOT NULL, nombre_tabla VARCHAR(100) NOT NULL, total_filas BIGINT, estadisticas JSONB);
CREATE TABLE IF NOT EXISTS DQM.reglas_calidad (regla_id SERIAL PRIMARY KEY, nombre_regla VARCHAR(255) UNIQUE NOT NULL, descripcion TEXT, entidad_objetivo VARCHAR(100) NOT NULL, campo_objetivo VARCHAR(100), tipo_regla VARCHAR(50) CHECK (tipo_regla IN ('Validez', 'Completitud', 'Consistencia', 'Unicidad')), umbral_error_porcentaje NUMERIC(5, 2) DEFAULT 0.00, query_sql_validacion TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS DQM.resultados_calidad (resultado_id SERIAL PRIMARY KEY, regla_id INTEGER NOT NULL REFERENCES DQM.reglas_calidad(regla_id), log_proceso_id INTEGER REFERENCES DQM.log_procesos(log_id), fecha_ejecucion TIMESTAMP WITH TIME ZONE NOT NULL, total_registros_evaluados BIGINT, total_registros_fallidos BIGINT, porcentaje_fallo NUMERIC(5, 2), resultado_final VARCHAR(50) CHECK (resultado_final IN ('Aprobado', 'Rechazado', 'Advertencia')));


-- =====================================================================
-- PASO 1.5: AÑADIR RESTRICCIONES UNIQUE A LAS TABLAS DE METADATA
-- =====================================================================
DO $$
BEGIN
    -- Hacemos que el nombre de la entidad sea único.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_md_entities_entity_name' AND conrelid = 'md_entities'::regclass) THEN
        ALTER TABLE md_entities ADD CONSTRAINT uq_md_entities_entity_name UNIQUE (entity_name);
    END IF;

    -- Hacemos que la combinación de entidad y columna sea única.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_md_attributes_entity_column' AND conrelid = 'md_attributes'::regclass) THEN
        ALTER TABLE md_attributes ADD CONSTRAINT uq_md_attributes_entity_column UNIQUE (entity_id, column_name);
    END IF;

    -- Hacemos que cada definición de relación sea única.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_md_relationships_columns' AND conrelid = 'md_relationships'::regclass) THEN
        ALTER TABLE md_relationships ADD CONSTRAINT uq_md_relationships_columns UNIQUE (parent_entity, parent_column, child_entity, child_column);
    END IF;
END;
$$;


-- =====================================================================
-- PASO 2: DOCUMENTACIÓN COMPLETA DEL DQM EN LA METADATA
-- =====================================================================

-- 2.1: Registrar Entidades en `md_entities`
INSERT INTO md_entities (entity_name, business_name, layer, entity_type, grain, primary_key, description, is_active, created_by) VALUES
('log_procesos', 'Log de Procesos ETL', 'DQM', 'Log', 'Una fila por cada ejecución de un proceso.', 'log_id', 'Tabla de bitácora que registra la ejecución, estado y resultado de todos los procesos.', TRUE, 'Admin'),
('perfilado_entidades', 'Perfilado de Entidades', 'DQM', 'Estadística', 'Una fila por cada tabla perfilada en una ejecución.', 'perfilado_id', 'Almacena estadísticas descriptivas de las tablas del DWA.', TRUE, 'Admin'),
('reglas_calidad', 'Catálogo de Reglas de Calidad', 'DQM', 'Catálogo', 'Una fila por cada regla de negocio de calidad definida.', 'regla_id', 'Catálogo central que define las reglas de negocio para la calidad de datos.', TRUE, 'Admin'),
('resultados_calidad', 'Resultados de Validaciones', 'DQM', 'Log', 'Una fila por cada ejecución de una regla de calidad.', 'resultado_id', 'Registra el resultado histórico de la ejecución de cada regla de calidad.', TRUE, 'Admin')
ON CONFLICT (entity_name) DO UPDATE SET description = EXCLUDED.description;


-- 2.2: Registrar Atributos en `md_attributes`
DO $$
DECLARE v_entity_id INTEGER;
BEGIN
    SELECT entity_id INTO v_entity_id FROM md_entities WHERE entity_name = 'log_procesos';
    INSERT INTO md_attributes (entity_id, column_name, data_type, description, is_primary_key, is_foreign_key, is_nullable, created_by) VALUES
    (v_entity_id, 'log_id', 'SERIAL', 'ID único del log.', TRUE, FALSE, FALSE, 'Admin'),
    (v_entity_id, 'proceso_nombre', 'VARCHAR(255)', 'Nombre del proceso ejecutado.', FALSE, FALSE, FALSE, 'Admin'),
    (v_entity_id, 'fecha_inicio', 'TIMESTAMP', 'Inicio de la ejecución.', FALSE, FALSE, FALSE, 'Admin')
    ON CONFLICT (entity_id, column_name) DO NOTHING;
END $$;

-- 2.3: Registrar Relaciones en `md_relationships`
INSERT INTO md_relationships (parent_entity, parent_column, child_entity, child_column, relationship_type, description, created_by) VALUES
('reglas_calidad', 'regla_id', 'resultados_calidad', 'regla_id', '1-a-N', 'Cada resultado se deriva de una única regla.', 'Admin'),
('log_procesos', 'log_id', 'resultados_calidad', 'log_proceso_id', '1-a-N', 'Una ejecución puede generar múltiples validaciones.', 'Admin'),
('log_procesos', 'log_id', 'perfilado_entidades', 'log_proceso_id', '1-a-N', 'Una ejecución puede generar el perfilado de tablas.', 'Admin')
ON CONFLICT (parent_entity, parent_column, child_entity, child_column) DO NOTHING;

-- =====================================================================
-- PASO 3: DEFINICIÓN DE REGLAS DE CALIDAD ADAPTADAS A TU MODELO
-- =====================================================================
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

-- =====================================================================
-- PASO 4: CREACIÓN DEL PROCEDIMIENTO DINÁMICO DE CHEQUEO 
-- =====================================================================
CREATE OR REPLACE PROCEDURE DQM.ejecutar_chequeos_calidad(p_nombre_tabla TEXT, p_log_id INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    regla RECORD;
    v_total_filas BIGINT;
    v_filas_fallidas BIGINT;
    v_porc_fallo NUMERIC;
    v_resultado TEXT;
BEGIN
    -- Iteramos sobre todas las reglas definidas para la tabla especificada.
    FOR regla IN SELECT * FROM DQM.reglas_calidad WHERE entidad_objetivo = p_nombre_tabla LOOP
        -- Obtenemos el total de filas de la tabla objetivo para el cálculo del porcentaje.
        EXECUTE format('SELECT COUNT(*) FROM %I', regla.entidad_objetivo) INTO v_total_filas;

        -- Ejecutamos dinámicamente el query de validación de la regla para contar los fallos.
        EXECUTE regla.query_sql_validacion INTO v_filas_fallidas;

        -- Calculamos el porcentaje de fallo de forma segura.
        IF v_total_filas > 0 THEN
            v_porc_fallo := (v_filas_fallidas::NUMERIC / v_total_filas) * 100;
        ELSE
            v_porc_fallo := 0;
        END IF;

        -- Determinamos el resultado final comparando con el umbral definido en la regla.
        IF v_porc_fallo > regla.umbral_error_porcentaje THEN
            v_resultado := 'Rechazado';
        ELSE
            v_resultado := 'Aprobado';
        END IF;

        -- Insertamos el resultado detallado en nuestra tabla de resultados.
        INSERT INTO DQM.resultados_calidad (regla_id, log_proceso_id, fecha_ejecucion, total_registros_evaluados, total_registros_fallidos, porcentaje_fallo, resultado_final)
        VALUES (regla.regla_id, p_log_id, NOW(), v_total_filas, v_filas_fallidas, v_porc_fallo, v_resultado);

        -- LÍNEA CORREGIDA: Se eliminó el último parámetro '%' que causaba el error.
        RAISE NOTICE 'Regla "%" ejecutada. Resultado: %. Fallos: % de % (% %%)', regla.nombre_regla, v_resultado, v_filas_fallidas, v_total_filas, round(v_porc_fallo, 2);
    END LOOP;
END;
$$;
