DO $$
DECLARE
    v_log_id BIGINT;
    v_script_id INT := 1;  -- ejemplo
   v_total_validated_rows BIGINT := 0;
    v_current_table_rows BIGINT;
	v_records RECORD;	
    v_msg text;
    v_detail text;
    v_hint text;
    v_context text;
    v_script_name TEXT :=  'E2_03_dwm_insert_data.sql';
BEGIN
  -- Registrar el script en el inventario
  SELECT script_id INTO v_script_id 
  FROM dqm_scripts_inventory WHERE script_name = v_script_name;

  -- Si no existe (es NULL), lo creamos
  IF v_script_id IS NULL THEN
    INSERT INTO dqm_scripts_inventory (script_name, description, created_by, created_at)
    VALUES (
      v_script_name, 
      'Insertar datos en las tablas de la capa de memoria',
      'Sarah',
      NOW()
    )
    RETURNING script_id INTO v_script_id;
  END IF;

-- 2. Registrar el inicio del log
  INSERT INTO dqm_exec_log (script_id, started_at, status)
    VALUES (v_script_id, NOW(), 'RUNNING')
    RETURNING log_id INTO v_log_id;


-- ==========================================================
    -- 2. BLOQUE PRINCIPAL DE LÓGICA (PROTEGIDO POR EXCEPCIÓN)
    -- ==========================================================
    BEGIN


        -- 1. CARGA DE DWM_CATEGORIES
        TRUNCATE TABLE dwm_categories;
        INSERT INTO dwm_categories (category_id, category_name, description, picture, load_date)
        SELECT category_id, category_name, description, picture, NOW()
        FROM tmp_categories;

        -- 2. CARGA DE DWM_SUPPLIERS
        TRUNCATE TABLE dwm_suppliers;
        INSERT INTO dwm_suppliers (supplier_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, homepage, load_date)
        SELECT supplier_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, homepage, NOW()
        FROM tmp_suppliers;

        -- 3. CARGA DE DWM_PRODUCTS
        TRUNCATE TABLE dwm_products;
        INSERT INTO dwm_products (product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued, load_date)
        SELECT product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued, NOW()
        FROM tmp_products;

        -- 4. CARGA DE DWM_CUSTOMERS
        TRUNCATE TABLE dwm_customers;
        INSERT INTO dwm_customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, load_date)
        SELECT customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, NOW()
        FROM tmp_customers;

        -- 5. CARGA DE DWM_EMPLOYEES
        TRUNCATE TABLE dwm_employees;
        INSERT INTO dwm_employees (employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, extension, photo, notes, reports_to, photo_path, load_date)
        SELECT employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, extension, photo, notes, reports_to, photo_path, NOW()
        FROM tmp_employees;

        -- 6. CARGA DE DWM_ORDERS
        TRUNCATE TABLE dwm_orders;
        INSERT INTO dwm_orders (order_id, customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country, load_date)
        SELECT order_id, customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country, NOW()
        FROM tmp_orders;

        -- 7. CARGA DE DWM_ORDER_DETAILS
        TRUNCATE TABLE dwm_order_details;
        INSERT INTO dwm_order_details (order_id, product_id, unit_price, quantity, discount, load_date)
        SELECT order_id, product_id, unit_price, quantity, discount, NOW()
        FROM tmp_order_details;

        -- 8. CARGA DE DWM_SHIPPERS
        TRUNCATE TABLE dwm_shippers;
        INSERT INTO dwm_shippers (shipper_id, company_name, phone, load_date)
        SELECT shipper_id, company_name, phone, NOW()
        FROM tmp_shippers;

        -- 9. CARGA DE DWM_REGION
        TRUNCATE TABLE dwm_region;
        INSERT INTO dwm_region (region_id, region_description, load_date)
        SELECT region_id, region_description, NOW()
        FROM tmp_region;

        -- 10. CARGA DE DWM_TERRITORIES
        TRUNCATE TABLE dwm_territories;
        INSERT INTO dwm_territories (territory_id, territory_description, region_id, load_date)
        SELECT territory_id, territory_description, region_id, NOW()
        FROM tmp_territories;

        -- 11. CARGA DE DWM_EMPLOYEE_TERRITORIES
        TRUNCATE TABLE dwm_employee_territories;
        INSERT INTO dwm_employee_territories (employee_id, territory_id, load_date)
        SELECT employee_id, territory_id, NOW()
        FROM tmp_employee_territories;




-- Actualizar contador de filas procesadas
        SELECT 
                (SELECT COUNT(*) FROM dwm_categories) +
                (SELECT COUNT(*) FROM dwm_suppliers) +
                (SELECT COUNT(*) FROM dwm_products) +
                (SELECT COUNT(*) FROM dwm_customers) +
                (SELECT COUNT(*) FROM dwm_employees) +
                (SELECT COUNT(*) FROM dwm_orders) +
                (SELECT COUNT(*) FROM dwm_order_details) +
                (SELECT COUNT(*) FROM dwm_shippers) +
                (SELECT COUNT(*) FROM dwm_region) +
                (SELECT COUNT(*) FROM dwm_territories) +
                (SELECT COUNT(*) FROM dwm_employee_territories)
            INTO v_total_processed_rows;




    -- =======================================================================    
-- Si esta sección finaliza sin error, actualiza el log como 'OK'
        UPDATE dqm_exec_log
        SET finished_at = NOW(),
            status = 'OK',
            message = 'Completado exitosamente.',
            rows_processed = v_total_validated_rows -- El valor acumulado
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
                status = 'CRITICAL_ERROR', -- Diferente a error de datos
                message = 'Fallo Técnico: ' || v_msg || ' Detalle: ' || v_detail
            WHERE log_id = v_log_id;

            -- IMPORTANTE: NO hacemos RAISE EXCEPTION aquí.
            -- Hacemos RAISE NOTICE para que el script termine "bien" a ojos de SQL
            -- y se guarde el INSERT/UPDATE del log.
            RAISE NOTICE 'El script falló técnicamente. Revisa dqm_exec_log ID %', v_log_id;
            
    END; -- Fin del bloque principal

END $$;