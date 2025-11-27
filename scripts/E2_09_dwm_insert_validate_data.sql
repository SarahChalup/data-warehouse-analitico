-- =====================================================================
-- E2_09_DWM_INSERT_VALIDATE_DATA
-- Objetivo: Validar reglas DQM y persistir en Memoria (DWM) controlando excepciones al final.
-- =====================================================================

DO $$
DECLARE
    -- === Bloque de Logging y Variables ===
    v_script_nombre TEXT := 'E2_09_dwm_insert_validate_data.sql';
    v_script_desc   TEXT := 'Validación de Quality Gate y carga de tablas DWM.';
    v_created_by    TEXT := 'Sarah';
    v_log_id BIGINT; v_script_id INT; 
    
    -- Variables para manejo de errores
    v_msg TEXT; v_detail TEXT;
    
    -- Variables de lógica
    v_cant_rechazos INTEGER;
    v_total_validated_rows INTEGER := 0; -- Acumulador simple (opcional)

BEGIN
    -- 1. INICIO DEL LOG
    SELECT script_id INTO v_script_id FROM dqm_scripts_inventory WHERE script_name = v_script_nombre;
    IF v_script_id IS NULL THEN
        INSERT INTO dqm_scripts_inventory (script_name, description, created_by) VALUES (v_script_nombre, v_script_desc, v_created_by) RETURNING script_id INTO v_script_id;
    END IF;
    INSERT INTO dqm_exec_log (script_id, started_at, status) VALUES (v_script_id, NOW(), 'RUNNING') RETURNING log_id INTO v_log_id;

    -- BLOQUE PRINCIPAL (Todo ocurre aquí adentro)
    BEGIN
        RAISE NOTICE '--- INICIO FASE 1: CONTROL DE CALIDAD Y CARGA A MEMORIA ---';

        -- 1. CATEGORIAS
        SELECT COUNT(*) INTO v_cant_rechazos FROM dqm_resultados_calidad res JOIN dqm_reglas_calidad reg ON res.regla_id = reg.regla_id 
        WHERE reg.entidad_objetivo = 'tmp_categories' AND res.resultado_final = 'Rechazado' AND res.fecha_ejecucion >= (NOW() - INTERVAL '1 hour');
        
        IF v_cant_rechazos > 0 THEN RAISE WARNING '[BLOQUEADO] Categories: % rechazos.', v_cant_rechazos; ELSE
            TRUNCATE TABLE dwm_categories;
            INSERT INTO dwm_categories (category_id, category_name, description, picture, load_date)
            SELECT category_id, category_name, description, picture, NOW() FROM tmp_categories;
        END IF;

        -- 2. PROVEEDORES
        SELECT COUNT(*) INTO v_cant_rechazos FROM dqm_resultados_calidad res JOIN dqm_reglas_calidad reg ON res.regla_id = reg.regla_id 
        WHERE reg.entidad_objetivo = 'tmp_suppliers' AND res.resultado_final = 'Rechazado' AND res.fecha_ejecucion >= (NOW() - INTERVAL '1 hour');
        
        IF v_cant_rechazos > 0 THEN RAISE WARNING '[BLOQUEADO] Suppliers: % rechazos.', v_cant_rechazos; ELSE
            TRUNCATE TABLE dwm_suppliers;
            INSERT INTO dwm_suppliers (supplier_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, homepage, load_date)
            SELECT supplier_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, homepage, NOW() FROM tmp_suppliers;
        END IF;

        -- 3. PRODUCTOS
        SELECT COUNT(*) INTO v_cant_rechazos FROM dqm_resultados_calidad res JOIN dqm_reglas_calidad reg ON res.regla_id = reg.regla_id 
        WHERE reg.entidad_objetivo = 'tmp_products' AND res.resultado_final = 'Rechazado' AND res.fecha_ejecucion >= (NOW() - INTERVAL '1 hour');
        
        IF v_cant_rechazos > 0 THEN RAISE WARNING '[BLOQUEADO] Products: % rechazos.', v_cant_rechazos; ELSE
            TRUNCATE TABLE dwm_products;
            INSERT INTO dwm_products (product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued, load_date)
            SELECT product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued, NOW() FROM tmp_products;
        END IF;

        -- 4. CLIENTES
        SELECT COUNT(*) INTO v_cant_rechazos FROM dqm_resultados_calidad res JOIN dqm_reglas_calidad reg ON res.regla_id = reg.regla_id 
        WHERE reg.entidad_objetivo = 'tmp_customers' AND res.resultado_final = 'Rechazado' AND res.fecha_ejecucion >= (NOW() - INTERVAL '1 hour');
        
        IF v_cant_rechazos > 0 THEN RAISE WARNING '[BLOQUEADO] Customers: % rechazos.', v_cant_rechazos; ELSE
            TRUNCATE TABLE dwm_customers;
            INSERT INTO dwm_customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, load_date)
            SELECT customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, NOW() FROM tmp_customers;
        END IF;

        -- 5. EMPLEADOS
        SELECT COUNT(*) INTO v_cant_rechazos FROM dqm_resultados_calidad res JOIN dqm_reglas_calidad reg ON res.regla_id = reg.regla_id 
        WHERE reg.entidad_objetivo = 'tmp_employees' AND res.resultado_final = 'Rechazado' AND res.fecha_ejecucion >= (NOW() - INTERVAL '1 hour');
        
        IF v_cant_rechazos > 0 THEN RAISE WARNING '[BLOQUEADO] Employees: % rechazos.', v_cant_rechazos; ELSE
            TRUNCATE TABLE dwm_employees;
            INSERT INTO dwm_employees (employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, extension, photo, notes, reports_to, photo_path, load_date)
            SELECT employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, extension, photo, notes, reports_to, photo_path, NOW() FROM tmp_employees;
        END IF;

        -- 6 y 7. VENTAS (Orders y Details)
        SELECT COUNT(*) INTO v_cant_rechazos FROM dqm_resultados_calidad res JOIN dqm_reglas_calidad reg ON res.regla_id = reg.regla_id 
        WHERE reg.entidad_objetivo IN ('tmp_orders', 'tmp_order_details') AND res.resultado_final = 'Rechazado' AND res.fecha_ejecucion >= (NOW() - INTERVAL '1 hour');
        
        IF v_cant_rechazos > 0 THEN RAISE WARNING '[BLOQUEADO] Orders/Details: % rechazos.', v_cant_rechazos; ELSE
            TRUNCATE TABLE dwm_orders;
            INSERT INTO dwm_orders (order_id, customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country, load_date)
            SELECT order_id, customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country, NOW() FROM tmp_orders;
            
            TRUNCATE TABLE dwm_order_details;
            INSERT INTO dwm_order_details (order_id, product_id, unit_price, quantity, discount, load_date)
            SELECT order_id, product_id, unit_price, quantity, discount, NOW() FROM tmp_order_details;
        END IF;

        -- 8. SHIPPERS
        SELECT COUNT(*) INTO v_cant_rechazos FROM dqm_resultados_calidad res JOIN dqm_reglas_calidad reg ON res.regla_id = reg.regla_id 
        WHERE reg.entidad_objetivo = 'tmp_shippers' AND res.resultado_final = 'Rechazado' AND res.fecha_ejecucion >= (NOW() - INTERVAL '1 hour');
        IF v_cant_rechazos > 0 THEN RAISE WARNING '[BLOQUEADO] Shippers: % rechazos.', v_cant_rechazos; ELSE
            TRUNCATE TABLE dwm_shippers;
            INSERT INTO dwm_shippers (shipper_id, company_name, phone, load_date)
            SELECT shipper_id, company_name, phone, NOW() FROM tmp_shippers;
        END IF;

        -- 9. REGION
        SELECT COUNT(*) INTO v_cant_rechazos FROM dqm_resultados_calidad res JOIN dqm_reglas_calidad reg ON res.regla_id = reg.regla_id 
        WHERE reg.entidad_objetivo = 'tmp_region' AND res.resultado_final = 'Rechazado' AND res.fecha_ejecucion >= (NOW() - INTERVAL '1 hour');
        IF v_cant_rechazos > 0 THEN RAISE WARNING '[BLOQUEADO] Region: % rechazos.', v_cant_rechazos; ELSE
            TRUNCATE TABLE dwm_region;
            INSERT INTO dwm_region (region_id, region_description, load_date)
            SELECT region_id, region_description, NOW() FROM tmp_region;
        END IF;

        -- 10. TERRITORIES
        SELECT COUNT(*) INTO v_cant_rechazos FROM dqm_resultados_calidad res JOIN dqm_reglas_calidad reg ON res.regla_id = reg.regla_id 
        WHERE reg.entidad_objetivo = 'tmp_territories' AND res.resultado_final = 'Rechazado' AND res.fecha_ejecucion >= (NOW() - INTERVAL '1 hour');
        IF v_cant_rechazos > 0 THEN RAISE WARNING '[BLOQUEADO] Territories: % rechazos.', v_cant_rechazos; ELSE
            TRUNCATE TABLE dwm_territories;
            INSERT INTO dwm_territories (territory_id, territory_description, region_id, load_date)
            SELECT territory_id, territory_description, region_id, NOW() FROM tmp_territories;
        END IF;

        -- 11. EMPLOYEE TERRITORIES
        SELECT COUNT(*) INTO v_cant_rechazos FROM dqm_resultados_calidad res JOIN dqm_reglas_calidad reg ON res.regla_id = reg.regla_id 
        WHERE reg.entidad_objetivo = 'tmp_employee_territories' AND res.resultado_final = 'Rechazado' AND res.fecha_ejecucion >= (NOW() - INTERVAL '1 hour');
        IF v_cant_rechazos > 0 THEN RAISE WARNING '[BLOQUEADO] EmpTerritories: % rechazos.', v_cant_rechazos; ELSE
            TRUNCATE TABLE dwm_employee_territories;
            INSERT INTO dwm_employee_territories (employee_id, territory_id, load_date)
            SELECT employee_id, territory_id, NOW() FROM tmp_employee_territories;
        END IF;

 

        -- ==========================================================
        -- FINALIZACIÓN EXITOSA
        -- ==========================================================
        UPDATE dqm_exec_log
        SET finished_at = NOW(),
            status = 'OK',
            message = 'Completado exitosamente.',
            rows_processed = v_total_validated_rows
        WHERE log_id = v_log_id;

        RAISE NOTICE 'Script finalizado exitosamente sin errores.';

    -- ==========================================================
    -- 3. MANEJO DE EXCEPCIONES
    -- ==========================================================
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL;
            
            -- Actualizamos log a ERROR CRITICO
            UPDATE dqm_exec_log
            SET finished_at = NOW(),
                status = 'CRITICAL_ERROR',
                message = 'Fallo Técnico: ' || v_msg || ' Detalle: ' || v_detail
            WHERE log_id = v_log_id;

            RAISE NOTICE 'El script falló técnicamente. Revisa dqm_exec_log ID %', v_log_id;
            
    END; -- Fin del bloque principal

END $$;