-- =====================================================================
-- SCRIPT E3_10h: Orquestador de Actualización con Lógica SCD Tipo 1
-- Integra TU código de UPDATE/INSERT en el flujo transaccional completo.
-- =====================================================================
DO $$
DECLARE
    -- === Variables de Logging ===
    v_script_nombre TEXT := 'E3_10h_update_dwa_scd1.sql';
    v_script_desc   TEXT := 'Actualiza el DWA con Ingesta2 (SCD1), carga hechos y valida con DQM.';
    v_created_by    TEXT := 'Equipo DWH';
    v_log_id BIGINT; v_script_id INT; v_error_msg TEXT; v_detail TEXT;

    -- === Variables para la Lógica de Negocio ===
    v_rows INT;
    v_total_changes INT := 0;
    v_errores_criticos INT;
BEGIN
    -- >> INICIO DEL LOG <<
    SELECT script_id INTO v_script_id FROM dqm_scripts_inventory WHERE script_name = v_script_nombre;
    IF v_script_id IS NULL THEN INSERT INTO dqm_scripts_inventory (script_name, description, created_by) VALUES (v_script_nombre, v_script_desc, v_created_by) RETURNING script_id INTO v_script_id; END IF;
    INSERT INTO dqm_exec_log (script_id, started_at, status) VALUES (v_script_id, NOW(), 'RUNNING') RETURNING log_id INTO v_log_id;

    BEGIN

        RAISE NOTICE 'Agregando nueva primary key';

        -- ================================================================
        -- 1. TABLA DWM_PRODUCTS
        -- ================================================================

        -- Primero: Eliminar la restricción de PK actual (probablemente sobre product_id)
        ALTER TABLE dwm_products DROP CONSTRAINT IF EXISTS dwm_products_pkey;

        -- Segundo: Agregar la columna dwm_id y hacerla la nueva PK
        ALTER TABLE dwm_products 
        ADD COLUMN dwm_id SERIAL PRIMARY KEY;


        -- ================================================================
        -- 2. TABLA DWM_CUSTOMERS
        -- ================================================================
        -- Primero: Eliminar la restricción de PK actual (probablemente sobre customer_id)
        ALTER TABLE dwm_customers DROP CONSTRAINT IF EXISTS dwm_customers_pkey;

        -- Segundo: Agregar la columna dwm_id y hacerla la nueva PK
        ALTER TABLE dwm_customers 
        ADD COLUMN dwm_id SERIAL PRIMARY KEY;


        -- ================================================================
        -- 3. TABLA DWM_ORDERS
        -- ================================================================
        -- Primero: Eliminar la restricción de PK actual (probablemente sobre order_id)
        ALTER TABLE dwm_orders DROP CONSTRAINT IF EXISTS dwm_orders_pkey;

        -- Segundo: Agregar la columna dwm_id y hacerla la nueva PK
        ALTER TABLE dwm_orders 
        ADD COLUMN dwm_id SERIAL PRIMARY KEY;


        -- ================================================================
        -- 4. TABLA DWM_COUNTRIES
        -- ================================================================
        -- Caso Especial: Si creaste esta tabla con "id SERIAL" en el paso anterior,
        -- lo ideal es renombrarla. Si no, borramos y creamos.

        -- Opción A: Si ya tiene una PK llamada "dwm_countries_pkey"
        ALTER TABLE dwm_countries DROP CONSTRAINT IF EXISTS dwm_countries_pkey;

        -- Si tenías una columna 'id' vieja que ya no quieres usar como PK, puedes borrarla o ignorarla.
        -- Aquí agregamos dwm_id nueva:
        ALTER TABLE dwm_countries 
        ADD COLUMN dwm_id SERIAL PRIMARY KEY;



        --  (PUNTO c y f) APLICAR CAMBIOS EN LA CAPA DE MEMORIA (DWM)

        RAISE NOTICE 'Aplicando cambios (Altas y Modificaciones) a la capa DWM...';

        /****************************************************************
         * c.1 PRODUCTS
         ****************************************************************/

        -- MODIFICACIONES
        UPDATE dwm_products d
        SET active = FALSE
        FROM tmp_products t
        WHERE d.product_id = t.product_id
        AND d.active = TRUE -- Solo comparamos contra el registro vigente
        AND (
                d.product_name      IS DISTINCT FROM t.product_name OR
                d.supplier_id       IS DISTINCT FROM t.supplier_id OR
                d.category_id       IS DISTINCT FROM t.category_id OR
                d.quantity_per_unit IS DISTINCT FROM t.quantity_per_unit OR
                d.unit_price        IS DISTINCT FROM t.unit_price OR
                d.units_in_stock    IS DISTINCT FROM t.units_in_stock OR
                d.units_on_order    IS DISTINCT FROM t.units_on_order OR
                d.reorder_level     IS DISTINCT FROM t.reorder_level OR
                d.discontinued      IS DISTINCT FROM t.discontinued
            );

        INSERT INTO dwm_products (
            product_id, 
            product_name, 
            supplier_id, 
            category_id, 
            quantity_per_unit, 
            unit_price, 
            units_in_stock, 
            units_on_order, 
            reorder_level, 
            discontinued, 
            active,      -- Importante
            load_date    -- Importante
        )
        SELECT 
            t.product_id, 
            t.product_name, 
            t.supplier_id, 
            t.category_id, 
            t.quantity_per_unit, 
            t.unit_price, 
            t.units_in_stock, 
            t.units_on_order, 
            t.reorder_level, 
            t.discontinued, 
            TRUE,        -- El nuevo registro nace activo
            NOW()        -- Fecha de carga actual
        FROM tmp_products t
        WHERE NOT EXISTS (
            -- Esta subquery evita duplicar registros que NO cambiaron
            SELECT 1 
            FROM dwm_products d 
            WHERE d.product_id = t.product_id
            AND d.active = TRUE
            AND d.product_name      IS NOT DISTINCT FROM t.product_name
            AND d.supplier_id       IS NOT DISTINCT FROM t.supplier_id
            AND d.category_id       IS NOT DISTINCT FROM t.category_id
            AND d.quantity_per_unit IS NOT DISTINCT FROM t.quantity_per_unit
            AND d.unit_price        IS NOT DISTINCT FROM t.unit_price
            AND d.units_in_stock    IS NOT DISTINCT FROM t.units_in_stock
            AND d.units_on_order    IS NOT DISTINCT FROM t.units_on_order
            AND d.reorder_level     IS NOT DISTINCT FROM t.reorder_level
            AND d.discontinued      IS NOT DISTINCT FROM t.discontinued
        );





        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- ALTAS
        INSERT INTO dwm_products (
            product_id, product_name, supplier_id, category_id,
            quantity_per_unit, unit_price, units_in_stock, units_on_order,
            reorder_level, discontinued, load_date, active
        )
        SELECT
            t.product_id, t.product_name, t.supplier_id, t.category_id,
            t.quantity_per_unit, t.unit_price, t.units_in_stock, t.units_on_order,
            t.reorder_level, t.discontinued,
            NOW(), TRUE
        FROM tmp_products t
        LEFT JOIN dwm_products d
          ON d.product_id = t.product_id
         AND d.active = TRUE
        WHERE d.product_id IS NULL;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;


        /****************************************************************
         * c.2 CUSTOMERS
         ****************************************************************/
        -- MODIFICACIONES
        UPDATE dwm_customers d
        SET active = FALSE
        FROM tmp_customers t
        WHERE d.customer_id = t.customer_id
        AND d.active = TRUE
        AND (
                d.company_name  IS DISTINCT FROM t.company_name OR
                d.contact_name  IS DISTINCT FROM t.contact_name OR
                d.contact_title IS DISTINCT FROM t.contact_title OR
                d.address       IS DISTINCT FROM t.address OR
                d.city          IS DISTINCT FROM t.city OR
                d.region        IS DISTINCT FROM t.region OR
                d.postal_code   IS DISTINCT FROM t.postal_code OR
                d.country       IS DISTINCT FROM t.country OR
                d.phone         IS DISTINCT FROM t.phone OR
                d.fax           IS DISTINCT FROM t.fax
            );

        INSERT INTO dwm_customers (
            customer_id, 
            company_name, 
            contact_name, 
            contact_title, 
            address, 
            city, 
            region, 
            postal_code, 
            country, 
            phone, 
            fax, 
            active,     -- Campo de control
            load_date   -- Campo de control
        )
        SELECT 
            t.customer_id, 
            t.company_name, 
            t.contact_name, 
            t.contact_title, 
            t.address, 
            t.city, 
            t.region, 
            t.postal_code, 
            t.country, 
            t.phone, 
            t.fax, 
            TRUE,       -- Nace activo
            NOW()       -- Fecha de carga
        FROM tmp_customers t
        WHERE NOT EXISTS (
            -- Solo insertamos si NO existe ya un registro IDÉNTICO y ACTIVO
            -- Si el paso 1 desactivó el registro, esta condición dará TRUE (porque no hay activo) y se insertará el nuevo.
            SELECT 1 
            FROM dwm_customers d 
            WHERE d.customer_id = t.customer_id
            AND d.active = TRUE
            AND d.company_name  IS NOT DISTINCT FROM t.company_name
            AND d.contact_name  IS NOT DISTINCT FROM t.contact_name
            AND d.contact_title IS NOT DISTINCT FROM t.contact_title
            AND d.address       IS NOT DISTINCT FROM t.address
            AND d.city          IS NOT DISTINCT FROM t.city
            AND d.region        IS NOT DISTINCT FROM t.region
            AND d.postal_code   IS NOT DISTINCT FROM t.postal_code
            AND d.country       IS NOT DISTINCT FROM t.country
            AND d.phone         IS NOT DISTINCT FROM t.phone
            AND d.fax           IS NOT DISTINCT FROM t.fax
        );

     
             GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- ALTAS
        INSERT INTO dwm_customers (
            customer_id, company_name, contact_name, contact_title,
            address, city, region, postal_code, country, phone, fax,
            load_date, active
        )
        SELECT
            t.customer_id, t.company_name, t.contact_name, t.contact_title,
            t.address, t.city, t.region, t.postal_code, t.country, t.phone, t.fax,
            NOW(), TRUE
        FROM tmp_customers t
        LEFT JOIN dwm_customers d
          ON d.customer_id = t.customer_id
         AND d.active = TRUE
        WHERE d.customer_id IS NULL;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        /****************************************************************
         * c.3 ORDERS
         ****************************************************************/

        -- MODIFICACIONES
    UPDATE dwm_orders d
SET active = FALSE
FROM tmp_orders t
WHERE d.order_id = t.order_id
  AND d.active = TRUE
  AND (
        d.customer_id      IS DISTINCT FROM t.customer_id OR
        d.employee_id      IS DISTINCT FROM t.employee_id OR
        d.order_date       IS DISTINCT FROM t.order_date OR
        d.required_date    IS DISTINCT FROM t.required_date OR
        d.shipped_date     IS DISTINCT FROM t.shipped_date OR
        d.ship_via         IS DISTINCT FROM t.ship_via OR
        d.freight          IS DISTINCT FROM t.freight OR
        d.ship_name        IS DISTINCT FROM t.ship_name OR
        d.ship_address     IS DISTINCT FROM t.ship_address OR
        d.ship_city        IS DISTINCT FROM t.ship_city OR
        d.ship_region      IS DISTINCT FROM t.ship_region OR
        d.ship_postal_code IS DISTINCT FROM t.ship_postal_code OR
        d.ship_country     IS DISTINCT FROM t.ship_country
      );


      INSERT INTO dwm_orders (
    order_id, 
    customer_id, 
    employee_id, 
    order_date, 
    required_date, 
    shipped_date, 
    ship_via, 
    freight, 
    ship_name, 
    ship_address, 
    ship_city, 
    ship_region, 
    ship_postal_code, 
    ship_country, 
    active,     -- Campo de control
    load_date   -- Campo de control
)
SELECT 
    t.order_id, 
    t.customer_id, 
    t.employee_id, 
    t.order_date, 
    t.required_date, 
    t.shipped_date, 
    t.ship_via, 
    t.freight, 
    t.ship_name, 
    t.ship_address, 
    t.ship_city, 
    t.ship_region, 
    t.ship_postal_code, 
    t.ship_country, 
    TRUE,       -- Nace activo
    NOW()       -- Fecha de carga
FROM tmp_orders t
WHERE NOT EXISTS (
    -- Verificamos que no exista ya un registro IDÉNTICO y ACTIVO
    SELECT 1 
    FROM dwm_orders d 
    WHERE d.order_id = t.order_id
      AND d.active = TRUE
      -- Aquí usamos IS NOT DISTINCT FROM (que significa "es igual a")
      AND d.customer_id      IS NOT DISTINCT FROM t.customer_id
      AND d.employee_id      IS NOT DISTINCT FROM t.employee_id
      AND d.order_date       IS NOT DISTINCT FROM t.order_date
      AND d.required_date    IS NOT DISTINCT FROM t.required_date
      AND d.shipped_date     IS NOT DISTINCT FROM t.shipped_date
      AND d.ship_via         IS NOT DISTINCT FROM t.ship_via
      AND d.freight          IS NOT DISTINCT FROM t.freight
      AND d.ship_name        IS NOT DISTINCT FROM t.ship_name
      AND d.ship_address     IS NOT DISTINCT FROM t.ship_address
      AND d.ship_city        IS NOT DISTINCT FROM t.ship_city
      AND d.ship_region      IS NOT DISTINCT FROM t.ship_region
      AND d.ship_postal_code IS NOT DISTINCT FROM t.ship_postal_code
      AND d.ship_country     IS NOT DISTINCT FROM t.ship_country
);
    
    
    
    
    
    
            GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- ALTAS
        INSERT INTO dwm_orders (
            order_id, customer_id, employee_id,
            order_date, required_date, shipped_date,
            ship_via, freight,
            ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country,
            load_date, active
        )
        SELECT
            t.order_id, t.customer_id, t.employee_id,
            t.order_date, t.required_date, t.shipped_date,
            t.ship_via, t.freight,
            t.ship_name, t.ship_address, t.ship_city, t.ship_region, t.ship_postal_code, t.ship_country,
            NOW(), TRUE
        FROM tmp_orders t
        LEFT JOIN dwm_orders d
          ON d.order_id = t.order_id
         AND d.active = TRUE
        WHERE d.order_id IS NULL;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;


        /****************************************************************
         * c.4 ORDER_DETAILS
         ****************************************************************/

        -- MODIFICACIONES

        UPDATE dwm_order_details d
        SET active = FALSE
        FROM tmp_order_details t
        WHERE d.order_id = t.order_id
        AND d.product_id = t.product_id -- Clave Compuesta
        AND d.active = TRUE
        AND (
                d.unit_price IS DISTINCT FROM t.unit_price OR
                d.quantity   IS DISTINCT FROM t.quantity   OR
                d.discount   IS DISTINCT FROM t.discount
            ); 

        INSERT INTO dwm_order_details (
            order_id, 
            product_id, 
            unit_price, 
            quantity, 
            discount, 
            active,    -- Campo de control
            load_date  -- Campo de control
        )
        SELECT 
            t.order_id, 
            t.product_id, 
            t.unit_price, 
            t.quantity, 
            t.discount, 
            TRUE,      -- Nace activo
            NOW()      -- Fecha de carga
        FROM tmp_order_details t
        WHERE NOT EXISTS (
            -- Verificamos que no exista ya un registro IDÉNTICO y ACTIVO
            SELECT 1 
            FROM dwm_order_details d 
            WHERE d.order_id = t.order_id
            AND d.product_id = t.product_id
            AND d.active = TRUE
            -- Comparamos los atributos para evitar duplicados si no hubo cambios
            AND d.unit_price IS NOT DISTINCT FROM t.unit_price
            AND d.quantity   IS NOT DISTINCT FROM t.quantity
            AND d.discount   IS NOT DISTINCT FROM t.discount
        );


        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- ALTAS
        INSERT INTO dwm_order_details (
            order_id, product_id, unit_price, quantity, discount, load_date, active
        )
        SELECT
            t.order_id, t.product_id, t.unit_price, t.quantity, t.discount,
            NOW(), TRUE
        FROM tmp_order_details t
        LEFT JOIN dwm_order_details d
          ON d.order_id = t.order_id
         AND d.product_id = t.product_id
         AND d.active = TRUE
        WHERE d.order_id IS NULL;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        RAISE NOTICE '% cambios aplicados a la capa DWM.', v_total_changes;


        /****************************************************************
         * c.5 COUNTRIES
         ****************************************************************/
        INSERT INTO dwm_countries (
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
            longitude,
            active,     -- Campo de control
            load_date   -- Campo de control
        )
        SELECT 
            t.country_name,
            t.density_per_km2,
            t.abbreviation,
            t.agricultural_land_percent,
            t.land_area_km2,
            t.armed_forces_size,
            t.birth_rate,
            t.calling_code,
            t.capital_city,
            t.co2_emissions,
            t.cpi,
            t.cpi_change_percent,
            t.currency_code,
            t.fertility_rate,
            t.forested_area_percent,
            t.gasoline_price,
            t.gdp,
            t.gross_primary_education_enrollment_percent,
            t.gross_tertiary_education_enrollment_percent,
            t.infant_mortality,
            t.largest_city,
            t.life_expectancy,
            t.maternal_mortality_ratio,
            t.minimum_wage,
            t.official_language,
            t.out_of_pocket_health_expenditure_percent,
            t.physicians_per_thousand,
            t.population,
            t.labor_force_participation_percent,
            t.tax_revenue_percent,
            t.total_tax_rate_percent,
            t.unemployment_rate_percent,
            t.urban_population,
            t.latitude,
            t.longitude,
            TRUE,       -- Nace activo
            NOW()       -- Fecha de carga
        FROM tmp_countries t
        WHERE NOT EXISTS (
            -- Verificamos que no exista ya un registro IDÉNTICO y ACTIVO
            SELECT 1 
            FROM dwm_countries d 
            WHERE d.country_name = t.country_name
            AND d.active = TRUE
            AND d.density_per_km2                             IS NOT DISTINCT FROM t.density_per_km2
            AND d.abbreviation                                IS NOT DISTINCT FROM t.abbreviation
            AND d.agricultural_land_percent                   IS NOT DISTINCT FROM t.agricultural_land_percent
            AND d.land_area_km2                               IS NOT DISTINCT FROM t.land_area_km2
            AND d.armed_forces_size                           IS NOT DISTINCT FROM t.armed_forces_size
            AND d.birth_rate                                  IS NOT DISTINCT FROM t.birth_rate
            AND d.calling_code                                IS NOT DISTINCT FROM t.calling_code
            AND d.capital_city                                IS NOT DISTINCT FROM t.capital_city
            AND d.co2_emissions                               IS NOT DISTINCT FROM t.co2_emissions
            AND d.cpi                                         IS NOT DISTINCT FROM t.cpi
            AND d.cpi_change_percent                          IS NOT DISTINCT FROM t.cpi_change_percent
            AND d.currency_code                               IS NOT DISTINCT FROM t.currency_code
            AND d.fertility_rate                              IS NOT DISTINCT FROM t.fertility_rate
            AND d.forested_area_percent                       IS NOT DISTINCT FROM t.forested_area_percent
            AND d.gasoline_price                              IS NOT DISTINCT FROM t.gasoline_price
            AND d.gdp                                         IS NOT DISTINCT FROM t.gdp
            AND d.gross_primary_education_enrollment_percent  IS NOT DISTINCT FROM t.gross_primary_education_enrollment_percent
            AND d.gross_tertiary_education_enrollment_percent IS NOT DISTINCT FROM t.gross_tertiary_education_enrollment_percent
            AND d.infant_mortality                            IS NOT DISTINCT FROM t.infant_mortality
            AND d.largest_city                                IS NOT DISTINCT FROM t.largest_city
            AND d.life_expectancy                             IS NOT DISTINCT FROM t.life_expectancy
            AND d.maternal_mortality_ratio                    IS NOT DISTINCT FROM t.maternal_mortality_ratio
            AND d.minimum_wage                                IS NOT DISTINCT FROM t.minimum_wage
            AND d.official_language                           IS NOT DISTINCT FROM t.official_language
            AND d.out_of_pocket_health_expenditure_percent    IS NOT DISTINCT FROM t.out_of_pocket_health_expenditure_percent
            AND d.physicians_per_thousand                     IS NOT DISTINCT FROM t.physicians_per_thousand
            AND d.population                                  IS NOT DISTINCT FROM t.population
            AND d.labor_force_participation_percent           IS NOT DISTINCT FROM t.labor_force_participation_percent
            AND d.tax_revenue_percent                         IS NOT DISTINCT FROM t.tax_revenue_percent
            AND d.total_tax_rate_percent                      IS NOT DISTINCT FROM t.total_tax_rate_percent
            AND d.unemployment_rate_percent                   IS NOT DISTINCT FROM t.unemployment_rate_percent
            AND d.urban_population                            IS NOT DISTINCT FROM t.urban_population
            AND d.latitude                                    IS NOT DISTINCT FROM t.latitude
            AND d.longitude                                   IS NOT DISTINCT FROM t.longitude
        );

/*
        -- =================================================================
        -- (PUNTO h) RECONSTRUIR LA CAPA DIMENSIONAL DESDE DWM
        -- =================================================================
        RAISE NOTICE 'Reconstruyendo la capa Dimensional desde la capa DWM actualizada...';
        TRUNCATE TABLE dim_customer, dim_product, dim_category, dim_supplier RESTART IDENTITY CASCADE;
        
        INSERT INTO dim_category (category_id, category_name) SELECT category_id, category_name FROM dwm_categories WHERE active = TRUE;
        INSERT INTO dim_supplier (supplier_id, company_name) SELECT supplier_id, company_name FROM dwm_suppliers WHERE active = TRUE;
        INSERT INTO dim_product (product_id, product_name, category_key, supplier_key, discontinued)
        SELECT p.product_id, p.product_name, dc.category_key, ds.supplier_key, p.discontinued FROM dwm_products p LEFT JOIN dim_category dc ON p.category_id = dc.category_id LEFT JOIN dim_supplier ds ON p.supplier_id = ds.supplier_id WHERE p.active = TRUE;
        INSERT INTO dim_customer (customer_id, company_name, city, country, region)
        SELECT customer_id, company_name, city, country, region FROM dwm_customers WHERE active = TRUE;
        
        -- =================================================================
        -- (PUNTO g y h) CARGAR NUEVOS HECHOS EN fact_table
        -- =================================================================
        RAISE NOTICE 'Cargando nuevos hechos en fact_table...';
        INSERT INTO fact_table (product_key, customer_key, employee_key, date_key, quantity, unit_price, discount, total_amount)
        SELECT dp.product_key, dc.customer_key, de.employee_key, TO_CHAR(o.order_date::date, 'YYYYMMDD')::INT, od.quantity, od.unit_price, od.discount, (od.quantity * od.unit_price * (1 - od.discount))
        FROM tmp_order_details od JOIN tmp_orders o ON od.order_id = o.order_id
        LEFT JOIN dim_product dp ON od.product_id = dp.product_id
        LEFT JOIN dim_customer dc ON o.customer_id = dc.customer_id
        LEFT JOIN dim_employee de ON o.employee_id = de.employee_id;
*/
        -- =================================================================
        -- (PUNTO h, i) VERIFICAR REGLAS Y ACTUALIZAR DQM
        -- =================================================================
        RAISE NOTICE 'Ejecutando validaciones DQM post-actualización...';
        CALL ejecutar_chequeos_calidad('fact_table', v_log_id);

        SELECT COUNT(*) INTO v_errores_criticos FROM dqm_resultados_calidad r JOIN dqm_reglas_calidad q ON r.regla_id = q.regla_id WHERE r.log_id = v_log_id AND r.resultado_final = 'Rechazado' AND q.umbral_error_porcentaje = 0.00;

        IF v_errores_criticos > 0 THEN
            RAISE EXCEPTION 'Se encontraron % errores críticos de DQM. Revirtiendo la actualización...', v_errores_criticos;
        END IF;
        
        -- >> CIERRE EXITOSO DEL LOG <<
        UPDATE dqm_exec_log SET finished_at = NOW(), status = 'OK', message = 'Actualización (SCD1) completada y validada.', rows_processed = v_total_changes WHERE log_id = v_log_id;
        RAISE NOTICE 'Script % finalizado con ÉXITO.', v_script_nombre;
    EXCEPTION
        -- >> MANEJO DE ERRORES <<
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL;
            UPDATE dqm_exec_log SET finished_at = NOW(), status = 'CRITICAL_ERROR', message = 'Fallo en la actualización: ' || v_error_msg WHERE log_id = v_log_id;
            RAISE WARNING 'Script % falló. Transacción revertida. Revisa dqm_exec_log ID %', v_script_nombre, v_log_id;
    END;
END $$;