DO $$
DECLARE
    v_log_id BIGINT;
    v_script_id INT := 1;  -- ejemplo
   v_total_rows BIGINT := 0;
    v_current_table_rows BIGINT;
	v_records RECORD;	
    v_msg text;
    v_detail text;
    v_hint text;
    v_context text;
    v_script_name TEXT :=  'E3_07_tmp_insert_data.sql';
BEGIN
  -- Registrar el script en el inventario
  SELECT script_id INTO v_script_id 
  FROM dqm_scripts_inventory WHERE script_name = v_script_name;

  -- Si no existe (es NULL), lo creamos
  IF v_script_id IS NULL THEN
    INSERT INTO dqm_scripts_inventory (script_name, description, created_by, created_at)
    VALUES (
      v_script_name, 
      'Corregir errores de formao en las tablas txt',
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


        -- Realizar la inserción de datos desde txt a tmp

        INSERT INTO tmp_categories (category_id, category_name, description, picture)
        SELECT 
            category_id::INTEGER, 
            category_name, 
            description, 
            -- Limpiamos el '0x' si existe y convertimos de hex a binario
            decode(replace(NULLIF(picture, 'NULL'), '0x', ''), 'hex')
        FROM txt_categories;

        GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
            v_total_rows = v_total_rows + v_current_table_rows;

        INSERT INTO tmp_suppliers (supplier_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, homepage)
        SELECT 
            supplier_id::INTEGER, 
            company_name, 
            contact_name, 
            contact_title, 
            address, 
            city, 
            NULLIF(region, 'NULL'), -- Manejo de nulos en texto
            postal_code, 
            country, 
            phone, 
            NULLIF(fax, 'NULL'), 
            NULLIF(home_page, 'NULL')
        FROM txt_suppliers;


        GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
            v_total_rows = v_total_rows + v_current_table_rows;

        INSERT INTO tmp_products (product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued)
        SELECT 
            product_id::INTEGER, 
            product_name, 
            NULLIF(supplier_id, 'NULL')::INTEGER, 
            NULLIF(category_id, 'NULL')::INTEGER, 
            quantity_per_unit, 
            NULLIF(unit_price, 'NULL')::NUMERIC(10,2), 
            NULLIF(units_in_stock, 'NULL')::INTEGER, 
            NULLIF(units_on_order, 'NULL')::INTEGER, 
            NULLIF(reorder_level, 'NULL')::INTEGER, 
            CASE 
                WHEN discontinued = 'TRUE' THEN 1 
                WHEN discontinued = 'FALSE' THEN 0 
                ELSE discontinued::INTEGER 
            END -- Manejo seguro para booleanos convertidos a int
        FROM txt_products;

        GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
        v_total_rows = v_total_rows + v_current_table_rows;


        INSERT INTO tmp_customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax)
        SELECT 
            customer_id, 
            customer_name, 
            contact_name, 
            contact_title, 
            address, 
            city, 
            NULLIF(regions, 'NULL'), 
            postal_code, 
            country, 
            phone, 
            NULLIF(fax, 'NULL')
        FROM txt_customers;

        GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	    v_total_rows = v_total_rows + v_current_table_rows;


        INSERT INTO tmp_employees (employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, extension, photo, notes, reports_to, photo_path)
        SELECT 
            employee_id::INTEGER, 
            last_name, 
            first_name, 
            title, 
            title_of_courtesy, 
            NULLIF(birth_date, 'NULL')::DATE, 
            NULLIF(hire_date, 'NULL')::DATE, 
            address, 
            city, 
            NULLIF(region, 'NULL'), 
            postal_code, 
            country, 
            home_phone, 
            extension, 
            decode(replace(NULLIF(photo, 'NULL'), '0x', ''), 'hex'), -- Conversión Bytea
            notes, 
            NULLIF(reports_to, 'NULL')::INTEGER, -- Importante el NULLIF aquí
            photo_path
        FROM txt_employees;

        GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	    v_total_rows = v_total_rows + v_current_table_rows;

        INSERT INTO tmp_orders (order_id, customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country)
        SELECT 
            order_id::INTEGER, 
            customer_id, 
            NULLIF(employee_id, 'NULL')::INTEGER, 
            NULLIF(order_date, 'NULL')::DATE, 
            NULLIF(required_date, 'NULL')::DATE, 
            NULLIF(shipped_date, 'NULL')::DATE, -- Convierte string 'NULL' a SQL NULL real
            NULLIF(ship_via, 'NULL')::INTEGER, 
            NULLIF(freight, 'NULL')::NUMERIC(10,2), 
            ship_name, 
            ship_address, 
            ship_city, 
            NULLIF(ship_region, 'NULL'), 
            ship_postal_code, 
            ship_country
        FROM txt_orders;

        GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	    v_total_rows = v_total_rows + v_current_table_rows;


        INSERT INTO tmp_order_details (order_id, product_id, unit_price, quantity, discount)
        SELECT 
            order_id::INTEGER, 
            product_id::INTEGER, 
            unit_price::NUMERIC(10,2), 
            quantity::INTEGER, 
            discount::NUMERIC(4,2)
        FROM txt_order_details;

        GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	    v_total_rows = v_total_rows + v_current_table_rows;


        INSERT INTO tmp_shippers (shipper_id, company_name, phone)
        SELECT 
            shipper_id::INTEGER, 
            company_name, 
            phone
        FROM txt_shippers;

        GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	    v_total_rows = v_total_rows + v_current_table_rows;


        INSERT INTO tmp_region (region_id, region_description)
        SELECT 
            region_id::INTEGER, 
            TRIM(region_description) -- El TRIM elimina espacios en blanco extras si los hay
        FROM txt_regions;

        GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	    v_total_rows = v_total_rows + v_current_table_rows;


        INSERT INTO tmp_territories (territory_id, territory_description, region_id)
        SELECT 
            territory_id, -- Es varchar, no necesita cast
            TRIM(territory_description), 
            region_id::INTEGER
        FROM txt_territories;

        GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	    v_total_rows = v_total_rows + v_current_table_rows;


        INSERT INTO tmp_employee_territories (employee_id, territory_id)
        SELECT 
            employee_id::INTEGER, 
            territory_id
        FROM txt_employee_territories;

        GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	    v_total_rows = v_total_rows + v_current_table_rows;



        INSERT INTO tmp_countries (
            country_name,
            density_per_km2,
            abbreviation,
            agricultural_land_percent,
            land_area_km2,
            armed_forces_size,
            birth_rate,
            calling_code,
            capital_city,
            co2_emissions,
            cpi,
            cpi_change_percent,
            currency_code,
            fertility_rate,
            forested_area_percent,
            gasoline_price,
            gdp,
            gross_primary_education_enrollment_percent,
            gross_tertiary_education_enrollment_percent,
            infant_mortality,
            largest_city,
            life_expectancy,
            maternal_mortality_ratio,
            minimum_wage,
            official_language,
            out_of_pocket_health_expenditure_percent,
            physicians_per_thousand,
            population,
            labor_force_participation_percent,
            tax_revenue_percent,
            total_tax_rate_percent,
            unemployment_rate_percent,
            urban_population,
            latitude,
            longitude
        )
        SELECT 
            country_name, -- Texto pasa directo
            NULLIF(density_per_km2, '')::NUMERIC,
            NULLIF(abbreviation, '')::VARCHAR(5),
            NULLIF(agricultural_land_percent, '')::NUMERIC,
            NULLIF(land_area_km2, '')::NUMERIC,
            NULLIF(armed_forces_size, '')::INTEGER,
            NULLIF(birth_rate, '')::NUMERIC,
            NULLIF(calling_code, '')::VARCHAR(10),
            capital_city, -- Texto pasa directo
            NULLIF(co2_emissions, '')::NUMERIC,
            NULLIF(cpi, '')::NUMERIC,
            NULLIF(cpi_change_percent, '')::NUMERIC,
            NULLIF(currency_code, '')::VARCHAR(10),
            NULLIF(fertility_rate, '')::NUMERIC,
            NULLIF(forested_area_percent, '')::NUMERIC,
            NULLIF(gasoline_price, '')::NUMERIC,
            NULLIF(gdp, '')::NUMERIC,
            NULLIF(gross_primary_education_enrollment_percent, '')::NUMERIC,
            NULLIF(gross_tertiary_education_enrollment_percent, '')::NUMERIC,
            NULLIF(infant_mortality, '')::NUMERIC,
            largest_city, -- Texto pasa directo
            NULLIF(life_expectancy, '')::NUMERIC,
            NULLIF(maternal_mortality_ratio, '')::INTEGER,
            NULLIF(minimum_wage, '')::NUMERIC,
            official_language, -- Texto pasa directo
            NULLIF(out_of_pocket_health_expenditure_percent, '')::NUMERIC,
            NULLIF(physicians_per_thousand, '')::NUMERIC,
            NULLIF(population, '')::BIGINT,
            NULLIF(labor_force_participation_percent, '')::NUMERIC,
            NULLIF(tax_revenue_percent, '')::NUMERIC,
            NULLIF(total_tax_rate_percent, '')::NUMERIC,
            NULLIF(unemployment_rate_percent, '')::NUMERIC,
            NULLIF(urban_population, '')::BIGINT,
            NULLIF(latitude, '')::NUMERIC,
            NULLIF(longitude, '')::NUMERIC
        FROM txt_countries;


      GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	    v_total_rows = v_total_rows + v_current_table_rows;

-- =======================================================================    
-- Si esta sección finaliza sin error, actualiza el log como 'OK'
        UPDATE dqm_exec_log
        SET finished_at = NOW(),
            status = 'OK',
            message = 'Completado exitosamente.',
            rows_processed = v_total_rows -- El valor acumulado
        WHERE log_id = v_log_id;

     RAISE NOTICE 'Script finalizado exitosamente sin errores.';
  
    -- ==========================================================
    -- 3. MANEJO DE EXCEPCIONES
    -- ==========================================================
    EXCEPTION
        WHEN OTHERS THEN
-- Capturar diagnóstico completo
            GET STACKED DIAGNOSTICS 
                v_msg = MESSAGE_TEXT, 
                v_detail = PG_EXCEPTION_DETAIL,
                v_hint = PG_EXCEPTION_HINT;
            
            -- Imprimir en consola INMEDIATAMENTE (mira la pestaña "Mensajes" en pgAdmin)
            RAISE NOTICE 'ERROR CAPTURADO: %', v_msg;
            RAISE NOTICE 'DETALLE: %', v_detail;            


-- Actualizamos log a ERROR CRITICO
            UPDATE dqm_exec_log
            SET finished_at = NOW(),
                status = 'CRITICAL_ERROR', -- Diferente a error de datos
                message = 'Fallo Técnico: ' || v_msg
            WHERE log_id = v_log_id;

    END; -- Fin del bloque principal

END $$;

