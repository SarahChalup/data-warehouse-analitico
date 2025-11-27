-- =====================================================================
-- E2_05_b: DEFINICIÓN DE REGLAS (FORMATO ESTÁNDAR EQUIPO)
-- Objetivo: Definir reglas de calidad con nomenclatura corporativa y ejecutarlas.
-- =====================================================================

DO $$
DECLARE
    v_script_nombre TEXT := 'E2_08_define_quality_rules';
    v_script_desc   TEXT := 'Definición de reglas de calidad (formato estandarizado) para las 13 tablas TMP.';
    v_created_by    TEXT := 'Data Engineer';
    v_log_id BIGINT; v_script_id INT; v_error_msg TEXT;
BEGIN
    -- 1. INICIO DEL LOG
    SELECT script_id INTO v_script_id FROM dqm_scripts_inventory WHERE script_name = v_script_nombre;
    IF v_script_id IS NULL THEN
        INSERT INTO dqm_scripts_inventory (script_name, description, created_by) VALUES (v_script_nombre, v_script_desc, v_created_by) RETURNING script_id INTO v_script_id;
    END IF;
    INSERT INTO dqm_exec_log (script_id, started_at, status) VALUES (v_script_id, NOW(), 'RUNNING') RETURNING log_id INTO v_log_id;

    RAISE NOTICE '--- INICIO: DEFINICIÓN DE REGLAS DE CALIDAD (ESTÁNDAR EQUIPO) ---';


    -- 3. INSERCIÓN DE REGLAS
    -- Formato: NOMBRE_REGLA (Mayúsculas) | Descripción Natural | Tabla | Campo | Tipo | Umbral | Query

    -- 1. Categorías
    INSERT INTO dqm_reglas_calidad (nombre_regla, descripcion, entidad_objetivo, campo_objetivo, tipo_regla, umbral_error_porcentaje, query_sql_validacion)
    VALUES ('CATEGORIA_ID_COMPLETITUD', 'El identificador de la categoría no puede ser nulo.', 'tmp_categories', 'category_id', 'Completitud', 1.00, 
            'SELECT COUNT(*) FROM tmp_categories WHERE category_id IS NULL');

    -- 2. Proveedores
    INSERT INTO dqm_reglas_calidad (nombre_regla, descripcion, entidad_objetivo, campo_objetivo, tipo_regla, umbral_error_porcentaje, query_sql_validacion)
    VALUES ('PROVEEDOR_ID_COMPLETITUD', 'El identificador del proveedor no puede ser nulo.', 'tmp_suppliers', 'supplier_id', 'Completitud', 1.00, 
            'SELECT COUNT(*) FROM tmp_suppliers WHERE supplier_id IS NULL');

    -- 3. Productos
    INSERT INTO dqm_reglas_calidad (nombre_regla, descripcion, entidad_objetivo, campo_objetivo, tipo_regla, umbral_error_porcentaje, query_sql_validacion)
    VALUES ('PRODUCTO_NOMBRE_COMPLETITUD', 'El nombre del producto no puede ser nulo o vacío.', 'tmp_products', 'product_name', 'Completitud', 1.00, 
            'SELECT COUNT(*) FROM tmp_products WHERE product_name IS NULL OR product_name = ''''');

    -- 4. Clientes
    INSERT INTO dqm_reglas_calidad (nombre_regla, descripcion, entidad_objetivo, campo_objetivo, tipo_regla, umbral_error_porcentaje, query_sql_validacion)
    VALUES ('CLIENTE_ID_COMPLETITUD', 'El identificador del cliente no puede ser nulo.', 'tmp_customers', 'customer_id', 'Completitud', 1.00, 
            'SELECT COUNT(*) FROM tmp_customers WHERE customer_id IS NULL');

    -- 5. Empleados
    INSERT INTO dqm_reglas_calidad (nombre_regla, descripcion, entidad_objetivo, campo_objetivo, tipo_regla, umbral_error_porcentaje, query_sql_validacion)
    VALUES ('EMPLEADO_ID_COMPLETITUD', 'El identificador del empleado no puede ser nulo.', 'tmp_employees', 'employee_id', 'Completitud', 1.00, 
            'SELECT COUNT(*) FROM tmp_employees WHERE employee_id IS NULL');

    -- 6. Ordenes (Orders)
    INSERT INTO dqm_reglas_calidad (nombre_regla, descripcion, entidad_objetivo, campo_objetivo, tipo_regla, umbral_error_porcentaje, query_sql_validacion)
    VALUES ('ORDEN_FECHA_COMPLETITUD', 'La fecha de la orden es obligatoria.', 'tmp_orders', 'order_date', 'Completitud', 1.00, 
            'SELECT COUNT(*) FROM tmp_orders WHERE order_date IS NULL');

    -- 7. Detalle Ordenes
    INSERT INTO dqm_reglas_calidad (nombre_regla, descripcion, entidad_objetivo, campo_objetivo, tipo_regla, umbral_error_porcentaje, query_sql_validacion)
    VALUES ('DETALLE_PRECIO_VALIDEZ', 'El precio unitario debe ser mayor o igual a cero.', 'tmp_order_details', 'unit_price', 'Validez', 0.00, 
            'SELECT COUNT(*) FROM tmp_order_details WHERE unit_price < 0');

    -- 8. Transportistas (Shippers)
    INSERT INTO dqm_reglas_calidad (nombre_regla, descripcion, entidad_objetivo, campo_objetivo, tipo_regla, umbral_error_porcentaje, query_sql_validacion)
    VALUES ('TRANSPORTISTA_ID_COMPLETITUD', 'El ID del transportista no puede ser nulo.', 'tmp_shippers', 'shipper_id', 'Completitud', 1.00, 
            'SELECT COUNT(*) FROM tmp_shippers WHERE shipper_id IS NULL');

    -- 9. Región
    INSERT INTO dqm_reglas_calidad (nombre_regla, descripcion, entidad_objetivo, campo_objetivo, tipo_regla, umbral_error_porcentaje, query_sql_validacion)
    VALUES ('REGION_ID_COMPLETITUD', 'El identificador de región es obligatorio.', 'tmp_region', 'region_id', 'Completitud', 1.00, 
            'SELECT COUNT(*) FROM tmp_region WHERE region_id IS NULL');

    -- 10. Territorios
    INSERT INTO dqm_reglas_calidad (nombre_regla, descripcion, entidad_objetivo, campo_objetivo, tipo_regla, umbral_error_porcentaje, query_sql_validacion)
    VALUES ('TERRITORIO_DESC_COMPLETITUD', 'La descripción del territorio no puede estar vacía.', 'tmp_territories', 'territory_description', 'Completitud', 1.00, 
            'SELECT COUNT(*) FROM tmp_territories WHERE territory_description IS NULL OR territory_description = ''''');

    -- 11. Empleado-Territorio
    INSERT INTO dqm_reglas_calidad (nombre_regla, descripcion, entidad_objetivo, campo_objetivo, tipo_regla, umbral_error_porcentaje, query_sql_validacion)
    VALUES ('EMP_TERR_RELACION_COMPLETITUD', 'La relación debe tener un ID de empleado válido.', 'tmp_employee_territories', 'employee_id', 'Completitud', 1.00, 
            'SELECT COUNT(*) FROM tmp_employee_territories WHERE employee_id IS NULL');

    -- 12. Demografía Clientes
    INSERT INTO dqm_reglas_calidad (nombre_regla, descripcion, entidad_objetivo, campo_objetivo, tipo_regla, umbral_error_porcentaje, query_sql_validacion)
    VALUES ('DEMOGRAFIA_TIPO_COMPLETITUD', 'El tipo de demografía es obligatorio.', 'tmp_customer_demographics', 'customer_type_id', 'Completitud', 1.00, 
            'SELECT COUNT(*) FROM tmp_customer_demographics WHERE customer_type_id IS NULL');

    -- 13. Relación Cliente-Demografía
    INSERT INTO dqm_reglas_calidad (nombre_regla, descripcion, entidad_objetivo, campo_objetivo, tipo_regla, umbral_error_porcentaje, query_sql_validacion)
    VALUES ('CLIENTE_DEMO_RELACION_COMPLETITUD', 'El ID del cliente en la relación no puede ser nulo.', 'tmp_customer_customer_demo', 'customer_id', 'Completitud', 1.00, 
            'SELECT COUNT(*) FROM tmp_customer_customer_demo WHERE customer_id IS NULL');


    RAISE NOTICE 'Reglas definidas con formato estándar.';

    -- 4. EJECUCIÓN DE CHEQUEOS (GENERACIÓN DE HISTORIAL)
    RAISE NOTICE '--- INICIO: EJECUCIÓN DE MOTOR DE CALIDAD ---';
    
    CALL ejecutar_chequeos_calidad('tmp_categories', v_log_id::INT);
    CALL ejecutar_chequeos_calidad('tmp_suppliers', v_log_id::INT);
    CALL ejecutar_chequeos_calidad('tmp_products', v_log_id::INT);
    CALL ejecutar_chequeos_calidad('tmp_customers', v_log_id::INT);
    CALL ejecutar_chequeos_calidad('tmp_employees', v_log_id::INT);
    CALL ejecutar_chequeos_calidad('tmp_orders', v_log_id::INT);
    CALL ejecutar_chequeos_calidad('tmp_order_details', v_log_id::INT);
    CALL ejecutar_chequeos_calidad('tmp_shippers', v_log_id::INT);
    CALL ejecutar_chequeos_calidad('tmp_region', v_log_id::INT);
    CALL ejecutar_chequeos_calidad('tmp_territories', v_log_id::INT);
    CALL ejecutar_chequeos_calidad('tmp_employee_territories', v_log_id::INT);
    CALL ejecutar_chequeos_calidad('tmp_customer_demographics', v_log_id::INT);
    CALL ejecutar_chequeos_calidad('tmp_customer_customer_demo', v_log_id::INT);

    -- 5. FIN DEL LOG
    UPDATE dqm_exec_log SET finished_at = NOW(), status = 'OK', message = 'Reglas estandarizadas definidas y ejecutadas.' WHERE log_id = v_log_id;
    RAISE NOTICE 'Proceso completado.';

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT;
        UPDATE dqm_exec_log SET finished_at = NOW(), status = 'CRITICAL_ERROR', message = 'Error: ' || v_error_msg WHERE log_id = v_log_id;
        RAISE WARNING 'Error en script. Ver log %', v_log_id;
END $$;