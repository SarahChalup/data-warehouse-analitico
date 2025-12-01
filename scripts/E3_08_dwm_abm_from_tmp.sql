-- =====================================================================
-- E3_08c_dwm_abm_from_tmp.sql
-- Punto 10.c - Altas, Bajas y Modificaciones (ABM) usando TMP (Ingesta2)
-- Tablas DWM involucradas:
--   dwm_categories, dwm_suppliers, dwm_products,
--   dwm_customers, dwm_employees,
--   dwm_orders, dwm_order_details,
--   dwm_shippers, dwm_region, dwm_territories, dwm_employee_territories
-- Requiere:
--   - TMP cargadas con E3_07_tmp_insert_data.sql
--   - Campo active agregado en tablas DWM (E3_02_truncate_tables.sql)
-- =====================================================================

DO $$
DECLARE
    v_script_name   TEXT := 'E3_10c_dwm_abm_from_tmp.sql';
    v_script_desc   TEXT := 'ABM (altas, bajas, modificaciones) sobre tablas DWM usando datos validados en TMP (Ingesta2).';
    v_created_by    TEXT := 'Mariana';

    v_script_id     INT;
    v_log_id        BIGINT;

    v_msg           TEXT;
    v_detail        TEXT;

    v_rows          BIGINT;
    v_total_changes BIGINT := 0;
BEGIN
    -------------------------------------------------------------------
    -- 1. Registrar script en dqm_scripts_inventory 
    -------------------------------------------------------------------
    SELECT script_id
    INTO   v_script_id
    FROM   dqm_scripts_inventory
    WHERE  script_name = v_script_name;

    IF v_script_id IS NULL THEN
        INSERT INTO dqm_scripts_inventory (
            script_name,
            description,
            created_by,
            created_at
        )
        VALUES (
            v_script_name,
            v_script_desc,
            v_created_by,
            NOW()
        )
        RETURNING script_id INTO v_script_id;
    END IF;

    -------------------------------------------------------------------
    -- 2. Registrar inicio en dqm_exec_log
    -------------------------------------------------------------------
    INSERT INTO dqm_exec_log (script_id, started_at, status, message)
    VALUES (v_script_id, NOW(), 'RUNNING', 'Inicio ABM DWM vs TMP (Ingesta2).')
    RETURNING log_id INTO v_log_id;

    -------------------------------------------------------------------
    -- 3. BLOQUE PRINCIPAL - ABM POR TABLA (protegido con EXCEPTION)
    -------------------------------------------------------------------
    BEGIN
        /****************************************************************
         * 3.1 CATEGORIES
         ****************************************************************/
        -- BAJAS: categorías activas en DWM que ya no vienen en TMP
        UPDATE dwm_categories d
        SET active = FALSE
        WHERE d.active = TRUE
          AND NOT EXISTS (
                SELECT 1
                FROM tmp_categories t
                WHERE t.category_id = d.category_id
          );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- MODIFICACIONES: cambio en nombre, descripción o picture
        UPDATE dwm_categories d
        SET
            category_name = t.category_name,
            description   = t.description,
            picture       = t.picture,
            load_date     = NOW()
        FROM tmp_categories t
        WHERE d.category_id = t.category_id
          AND d.active = TRUE
          AND (
                COALESCE(d.category_name, '') <> COALESCE(t.category_name, '') OR
                COALESCE(d.description,   '') <> COALESCE(t.description,   '') OR
                d.picture IS DISTINCT FROM t.picture
              );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- ALTAS: categorías en TMP que no existen activas en DWM
        INSERT INTO dwm_categories (
            category_id, category_name, description, picture, load_date, active
        )
        SELECT
            t.category_id,
            t.category_name,
            t.description,
            t.picture,
            NOW(),
            TRUE
        FROM tmp_categories t
        LEFT JOIN dwm_categories d
          ON d.category_id = t.category_id
         AND d.active = TRUE
        WHERE d.category_id IS NULL;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;


        /****************************************************************
         * 3.2 SUPPLIERS
         ****************************************************************/
        -- BAJAS
        UPDATE dwm_suppliers d
        SET active = FALSE
        WHERE d.active = TRUE
          AND NOT EXISTS (
                SELECT 1
                FROM tmp_suppliers t
                WHERE t.supplier_id = d.supplier_id
          );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- MODIFICACIONES
        UPDATE dwm_suppliers d
        SET
            company_name  = t.company_name,
            contact_name  = t.contact_name,
            contact_title = t.contact_title,
            address       = t.address,
            city          = t.city,
            region        = t.region,
            postal_code   = t.postal_code,
            country       = t.country,
            phone         = t.phone,
            fax           = t.fax,
            homepage      = t.homepage,
            load_date     = NOW()
        FROM tmp_suppliers t
        WHERE d.supplier_id = t.supplier_id
          AND d.active = TRUE
          AND (
                COALESCE(d.company_name,'')  <> COALESCE(t.company_name,'')  OR
                COALESCE(d.contact_name,'')  <> COALESCE(t.contact_name,'')  OR
                COALESCE(d.contact_title,'') <> COALESCE(t.contact_title,'') OR
                COALESCE(d.address,'')       <> COALESCE(t.address,'')       OR
                COALESCE(d.city,'')          <> COALESCE(t.city,'')          OR
                COALESCE(d.region,'')        <> COALESCE(t.region,'')        OR
                COALESCE(d.postal_code,'')   <> COALESCE(t.postal_code,'')   OR
                COALESCE(d.country,'')       <> COALESCE(t.country,'')       OR
                COALESCE(d.phone,'')         <> COALESCE(t.phone,'')         OR
                COALESCE(d.fax,'')           <> COALESCE(t.fax,'')           OR
                COALESCE(d.homepage,'')      <> COALESCE(t.homepage,'')
              );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- ALTAS
        INSERT INTO dwm_suppliers (
            supplier_id, company_name, contact_name, contact_title,
            address, city, region, postal_code, country,
            phone, fax, homepage, load_date, active
        )
        SELECT
            t.supplier_id, t.company_name, t.contact_name, t.contact_title,
            t.address, t.city, t.region, t.postal_code, t.country,
            t.phone, t.fax, t.homepage,
            NOW(), TRUE
        FROM tmp_suppliers t
        LEFT JOIN dwm_suppliers d
          ON d.supplier_id = t.supplier_id
         AND d.active = TRUE
        WHERE d.supplier_id IS NULL;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;


        /****************************************************************
         * 3.3 PRODUCTS
         ****************************************************************/
        -- BAJAS
        UPDATE dwm_products d
        SET active = FALSE
        WHERE d.active = TRUE
          AND NOT EXISTS (
                SELECT 1
                FROM tmp_products t
                WHERE t.product_id = d.product_id
          );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- MODIFICACIONES
        UPDATE dwm_products d
        SET
            product_name     = t.product_name,
            supplier_id      = t.supplier_id,
            category_id      = t.category_id,
            quantity_per_unit= t.quantity_per_unit,
            unit_price       = t.unit_price,
            units_in_stock   = t.units_in_stock,
            units_on_order   = t.units_on_order,
            reorder_level    = t.reorder_level,
            discontinued     = t.discontinued,
            load_date        = NOW()
        FROM tmp_products t
        WHERE d.product_id = t.product_id
          AND d.active = TRUE
          AND (
                COALESCE(d.product_name,'')      <> COALESCE(t.product_name,'')      OR
                d.supplier_id                    IS DISTINCT FROM t.supplier_id      OR
                d.category_id                    IS DISTINCT FROM t.category_id      OR
                COALESCE(d.quantity_per_unit,'') <> COALESCE(t.quantity_per_unit,'') OR
                d.unit_price                     IS DISTINCT FROM t.unit_price       OR
                d.units_in_stock                 IS DISTINCT FROM t.units_in_stock   OR
                d.units_on_order                 IS DISTINCT FROM t.units_on_order   OR
                d.reorder_level                  IS DISTINCT FROM t.reorder_level    OR
                d.discontinued                   IS DISTINCT FROM t.discontinued
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
         * 3.4 CUSTOMERS
         ****************************************************************/
        -- BAJAS
        UPDATE dwm_customers d
        SET active = FALSE
        WHERE d.active = TRUE
          AND NOT EXISTS (
                SELECT 1
                FROM tmp_customers t
                WHERE t.customer_id = d.customer_id
          );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- MODIFICACIONES
        UPDATE dwm_customers d
        SET
            company_name  = t.company_name,
            contact_name  = t.contact_name,
            contact_title = t.contact_title,
            address       = t.address,
            city          = t.city,
            region        = t.region,
            postal_code   = t.postal_code,
            country       = t.country,
            phone         = t.phone,
            fax           = t.fax,
            load_date     = NOW()
        FROM tmp_customers t
        WHERE d.customer_id = t.customer_id
          AND d.active = TRUE
          AND (
                COALESCE(d.company_name,'')  <> COALESCE(t.company_name,'')  OR
                COALESCE(d.contact_name,'')  <> COALESCE(t.contact_name,'')  OR
                COALESCE(d.contact_title,'') <> COALESCE(t.contact_title,'') OR
                COALESCE(d.address,'')       <> COALESCE(t.address,'')       OR
                COALESCE(d.city,'')          <> COALESCE(t.city,'')          OR
                COALESCE(d.region,'')        <> COALESCE(t.region,'')        OR
                COALESCE(d.postal_code,'')   <> COALESCE(t.postal_code,'')   OR
                COALESCE(d.country,'')       <> COALESCE(t.country,'')       OR
                COALESCE(d.phone,'')         <> COALESCE(t.phone,'')         OR
                COALESCE(d.fax,'')           <> COALESCE(t.fax,'')
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
         * 3.5 EMPLOYEES
         ****************************************************************/
        -- BAJAS
        UPDATE dwm_employees d
        SET active = FALSE
        WHERE d.active = TRUE
          AND NOT EXISTS (
                SELECT 1
                FROM tmp_employees t
                WHERE t.employee_id = d.employee_id
          );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- MODIFICACIONES
        UPDATE dwm_employees d
        SET
            last_name        = t.last_name,
            first_name       = t.first_name,
            title            = t.title,
            title_of_courtesy= t.title_of_courtesy,
            birth_date       = t.birth_date,
            hire_date        = t.hire_date,
            address          = t.address,
            city             = t.city,
            region           = t.region,
            postal_code      = t.postal_code,
            country          = t.country,
            home_phone       = t.home_phone,
            extension        = t.extension,
            photo            = t.photo,
            notes            = t.notes,
            reports_to       = t.reports_to,
            photo_path       = t.photo_path,
            load_date        = NOW()
        FROM tmp_employees t
        WHERE d.employee_id = t.employee_id
          AND d.active = TRUE
          AND (
                COALESCE(d.last_name,'')         <> COALESCE(t.last_name,'')         OR
                COALESCE(d.first_name,'')        <> COALESCE(t.first_name,'')        OR
                COALESCE(d.title,'')             <> COALESCE(t.title,'')             OR
                COALESCE(d.title_of_courtesy,'') <> COALESCE(t.title_of_courtesy,'') OR
                d.birth_date                     IS DISTINCT FROM t.birth_date       OR
                d.hire_date                      IS DISTINCT FROM t.hire_date        OR
                COALESCE(d.address,'')           <> COALESCE(t.address,'')           OR
                COALESCE(d.city,'')              <> COALESCE(t.city,'')              OR
                COALESCE(d.region,'')            <> COALESCE(t.region,'')            OR
                COALESCE(d.postal_code,'')       <> COALESCE(t.postal_code,'')       OR
                COALESCE(d.country,'')           <> COALESCE(t.country,'')           OR
                COALESCE(d.home_phone,'')        <> COALESCE(t.home_phone,'')        OR
                COALESCE(d.extension,'')         <> COALESCE(t.extension,'')         OR
                d.photo                          IS DISTINCT FROM t.photo            OR
                COALESCE(d.notes,'')             <> COALESCE(t.notes,'')             OR
                d.reports_to                     IS DISTINCT FROM t.reports_to       OR
                COALESCE(d.photo_path,'')        <> COALESCE(t.photo_path,'')
              );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- ALTAS
        INSERT INTO dwm_employees (
            employee_id, last_name, first_name, title, title_of_courtesy,
            birth_date, hire_date, address, city, region, postal_code, country,
            home_phone, extension, photo, notes, reports_to, photo_path,
            load_date, active
        )
        SELECT
            t.employee_id, t.last_name, t.first_name, t.title, t.title_of_courtesy,
            t.birth_date, t.hire_date, t.address, t.city, t.region, t.postal_code, t.country,
            t.home_phone, t.extension, t.photo, t.notes, t.reports_to, t.photo_path,
            NOW(), TRUE
        FROM tmp_employees t
        LEFT JOIN dwm_employees d
          ON d.employee_id = t.employee_id
         AND d.active = TRUE
        WHERE d.employee_id IS NULL;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;


        /****************************************************************
         * 3.6 ORDERS
         ****************************************************************/
        -- BAJAS
        UPDATE dwm_orders d
        SET active = FALSE
        WHERE d.active = TRUE
          AND NOT EXISTS (
                SELECT 1
                FROM tmp_orders t
                WHERE t.order_id = d.order_id
          );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- MODIFICACIONES
        UPDATE dwm_orders d
        SET
            customer_id      = t.customer_id,
            employee_id      = t.employee_id,
            order_date       = t.order_date,
            required_date    = t.required_date,
            shipped_date     = t.shipped_date,
            ship_via         = t.ship_via,
            freight          = t.freight,
            ship_name        = t.ship_name,
            ship_address     = t.ship_address,
            ship_city        = t.ship_city,
            ship_region      = t.ship_region,
            ship_postal_code = t.ship_postal_code,
            ship_country     = t.ship_country,
            load_date        = NOW()
        FROM tmp_orders t
        WHERE d.order_id = t.order_id
          AND d.active = TRUE
          AND (
                COALESCE(d.customer_id,'')      <> COALESCE(t.customer_id,'')      OR
                d.employee_id                   IS DISTINCT FROM t.employee_id     OR
                d.order_date                    IS DISTINCT FROM t.order_date      OR
                d.required_date                 IS DISTINCT FROM t.required_date   OR
                d.shipped_date                  IS DISTINCT FROM t.shipped_date    OR
                d.ship_via                      IS DISTINCT FROM t.ship_via        OR
                d.freight                       IS DISTINCT FROM t.freight         OR
                COALESCE(d.ship_name,'')        <> COALESCE(t.ship_name,'')        OR
                COALESCE(d.ship_address,'')     <> COALESCE(t.ship_address,'')     OR
                COALESCE(d.ship_city,'')        <> COALESCE(t.ship_city,'')        OR
                COALESCE(d.ship_region,'')      <> COALESCE(t.ship_region,'')      OR
                COALESCE(d.ship_postal_code,'') <> COALESCE(t.ship_postal_code,'') OR
                COALESCE(d.ship_country,'')     <> COALESCE(t.ship_country,'')
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
         * 3.7 ORDER_DETAILS
         ****************************************************************/
        -- BAJAS: por PK compuesta (order_id + product_id)
        UPDATE dwm_order_details d
        SET active = FALSE
        WHERE d.active = TRUE
          AND NOT EXISTS (
                SELECT 1
                FROM tmp_order_details t
                WHERE t.order_id  = d.order_id
                  AND t.product_id= d.product_id
          );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- MODIFICACIONES
        UPDATE dwm_order_details d
        SET
            unit_price  = t.unit_price,
            quantity    = t.quantity,
            discount    = t.discount,
            load_date   = NOW()
        FROM tmp_order_details t
        WHERE d.order_id   = t.order_id
          AND d.product_id = t.product_id
          AND d.active = TRUE
          AND (
                d.unit_price IS DISTINCT FROM t.unit_price OR
                d.quantity   IS DISTINCT FROM t.quantity   OR
                d.discount   IS DISTINCT FROM t.discount
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


        /****************************************************************
         * 3.8 SHIPPERS
         ****************************************************************/
        -- BAJAS
        UPDATE dwm_shippers d
        SET active = FALSE
        WHERE d.active = TRUE
          AND NOT EXISTS (
                SELECT 1
                FROM tmp_shippers t
                WHERE t.shipper_id = d.shipper_id
          );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- MODIFICACIONES
        UPDATE dwm_shippers d
        SET
            company_name = t.company_name,
            phone        = t.phone,
            load_date    = NOW()
        FROM tmp_shippers t
        WHERE d.shipper_id = t.shipper_id
          AND d.active = TRUE
          AND (
                COALESCE(d.company_name,'') <> COALESCE(t.company_name,'') OR
                COALESCE(d.phone,'')        <> COALESCE(t.phone,'')
              );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- ALTAS
        INSERT INTO dwm_shippers (
            shipper_id, company_name, phone, load_date, active
        )
        SELECT
            t.shipper_id, t.company_name, t.phone,
            NOW(), TRUE
        FROM tmp_shippers t
        LEFT JOIN dwm_shippers d
          ON d.shipper_id = t.shipper_id
         AND d.active = TRUE
        WHERE d.shipper_id IS NULL;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;


        /****************************************************************
         * 3.9 REGION
         ****************************************************************/
        -- BAJAS
        UPDATE dwm_region d
        SET active = FALSE
        WHERE d.active = TRUE
          AND NOT EXISTS (
                SELECT 1
                FROM tmp_region t
                WHERE t.region_id = d.region_id
          );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- MODIFICACIONES
        UPDATE dwm_region d
        SET
            region_description = t.region_description,
            load_date          = NOW()
        FROM tmp_region t
        WHERE d.region_id = t.region_id
          AND d.active = TRUE
          AND COALESCE(d.region_description,'') <> COALESCE(t.region_description,'');
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- ALTAS
        INSERT INTO dwm_region (
            region_id, region_description, load_date, active
        )
        SELECT
            t.region_id, t.region_description,
            NOW(), TRUE
        FROM tmp_region t
        LEFT JOIN dwm_region d
          ON d.region_id = t.region_id
         AND d.active = TRUE
        WHERE d.region_id IS NULL;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;


        /****************************************************************
         * 3.10 TERRITORIES
         ****************************************************************/
        -- BAJAS
        UPDATE dwm_territories d
        SET active = FALSE
        WHERE d.active = TRUE
          AND NOT EXISTS (
                SELECT 1
                FROM tmp_territories t
                WHERE t.territory_id = d.territory_id
          );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- MODIFICACIONES
        UPDATE dwm_territories d
        SET
            territory_description = t.territory_description,
            region_id             = t.region_id,
            load_date             = NOW()
        FROM tmp_territories t
        WHERE d.territory_id = t.territory_id
          AND d.active = TRUE
          AND (
                COALESCE(d.territory_description,'') <> COALESCE(t.territory_description,'') OR
                d.region_id IS DISTINCT FROM t.region_id
              );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- ALTAS
        INSERT INTO dwm_territories (
            territory_id, territory_description, region_id, load_date, active
        )
        SELECT
            t.territory_id, t.territory_description, t.region_id,
            NOW(), TRUE
        FROM tmp_territories t
        LEFT JOIN dwm_territories d
          ON d.territory_id = t.territory_id
         AND d.active = TRUE
        WHERE d.territory_id IS NULL;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;


        /****************************************************************
         * 3.11 EMPLOYEE_TERRITORIES
         ****************************************************************/
        -- BAJAS (PK compuesta)
        UPDATE dwm_employee_territories d
        SET active = FALSE
        WHERE d.active = TRUE
          AND NOT EXISTS (
                SELECT 1
                FROM tmp_employee_territories t
                WHERE t.employee_id  = d.employee_id
                  AND t.territory_id = d.territory_id
          );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;

        -- ALTAS (no hay otros atributos además de la PK)
        INSERT INTO dwm_employee_territories (
            employee_id, territory_id, load_date, active
        )
        SELECT
            t.employee_id, t.territory_id,
            NOW(), TRUE
        FROM tmp_employee_territories t
        LEFT JOIN dwm_employee_territories d
          ON d.employee_id  = t.employee_id
         AND d.territory_id = t.territory_id
         AND d.active = TRUE
        WHERE d.employee_id IS NULL;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total_changes := v_total_changes + v_rows;


        ----------------------------------------------------------------
        -- 4. Finalizar LOG en OK
        ----------------------------------------------------------------
        UPDATE dqm_exec_log
        SET finished_at   = NOW(),
            status        = 'OK',
            message       = format('ABM DWM vs TMP (Ingesta2) finalizado. Cambios totales: %s', v_total_changes),
            rows_processed = v_total_changes
        WHERE log_id = v_log_id;

        RAISE NOTICE 'E3_10c - ABM DWM completado. Cambios totales: %', v_total_changes;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT,
                                     v_detail = PG_EXCEPTION_DETAIL;

            UPDATE dqm_exec_log
            SET finished_at = NOW(),
                status      = 'CRITICAL_ERROR',
                message     = 'Error en E3_10c_dwm_abm_from_tmp: ' ||
                              COALESCE(v_msg,'') || ' Detalle: ' || COALESCE(v_detail,'')
            WHERE log_id = v_log_id;

            RAISE NOTICE 'El script E3_10c_dwm_abm_from_tmp falló. Revisar dqm_exec_log id=%', v_log_id;
    END; -- fin bloque principal

END $$;
