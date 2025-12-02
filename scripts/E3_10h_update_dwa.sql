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
        --  (PUNTO c y f) APLICAR CAMBIOS EN LA CAPA DE MEMORIA (DWM)

        RAISE NOTICE 'Aplicando cambios (Altas y Modificaciones) a la capa DWM...';

        /****************************************************************
         * c.1 PRODUCTS
         ****************************************************************/

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
         * c.2 CUSTOMERS
         ****************************************************************/
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
         * c.3 ORDERS
         ****************************************************************/

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
         * c.4 ORDER_DETAILS
         ****************************************************************/

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

        RAISE NOTICE '% cambios aplicados a la capa DWM.', v_total_changes;

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