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
    v_script_name TEXT :=  'E3_12_enr_update_view_ingesta2.sql';
BEGIN
  -- Registrar el script en el inventario
  SELECT script_id INTO v_script_id 
  FROM dqm_scripts_inventory WHERE script_name = v_script_name;

  -- Si no existe (es NULL), lo creamos
  IF v_script_id IS NULL THEN
    INSERT INTO dqm_scripts_inventory (script_name, description, created_by, created_at)
    VALUES (
      v_script_name, 
      'Actualizar vistas de la capa de enriquecimiento con datos de ingesta 2',
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

        -- =================================================================
       -- RELACIONES DE PRODUCTO (Snowflake)
        -- =================================================================

        -- Relación: Producto -> Categoría
        ALTER TABLE dim_product
        ADD CONSTRAINT fk_dim_product_category
        FOREIGN KEY (category_key) REFERENCES dim_category(category_key);

        -- Relación: Producto -> Proveedor
        ALTER TABLE dim_product
        ADD CONSTRAINT fk_dim_product_supplier
        FOREIGN KEY (supplier_key) REFERENCES dim_supplier(supplier_key);


        -- Relaciones de la Fact Table (Esquema Estrella)
        ALTER TABLE fact_table -- (o el nombre de tu tabla de hechos)
        ADD CONSTRAINT fk_fact_product FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
        ADD CONSTRAINT fk_fact_customer FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
        ADD CONSTRAINT fk_fact_employee FOREIGN KEY (employee_key) REFERENCES dim_employee(employee_key);
        -- ADD CONSTRAINT fk_fact_time ... (si tienes dimensión tiempo)

        -- =================================================================
       -- VISTAS DE LA CAPA DE ENRIQUECIMIENTO (ENR)
        -- =================================================================


                -- 1. VISTA ENRIQUECIDA DE PRODUCTOS (Solo Activos)
            CREATE OR REPLACE VIEW enr_dim_products AS
            SELECT 
                product_id,            -- Business Key
                product_name,
                supplier_id,
                category_id,
                quantity_per_unit,
                unit_price,
                CASE 
                    WHEN discontinued = 1 THEN TRUE 
                    ELSE FALSE 
                END AS is_discontinued
            FROM dwm_products
            WHERE active = TRUE; -- <<--- EL FILTRO MÁGICO


            -- 2. VISTA ENRIQUECIDA DE EMPLEADOS (Solo Activos)
            CREATE OR REPLACE VIEW enr_dim_employees AS
            SELECT 
                employee_id,
                first_name || ' ' || last_name AS full_name,
                title,
                city,
                country, -- OJO: Si ya normalizaste a country_key, ajusta esto. Si no, déjalo así.
                region,
                hire_date
            FROM dwm_employees
            WHERE active = TRUE; -- <<--- EL FILTRO MÁGICO


            -- 3. VISTAS DE PASO DIRECTO (Solo Activos)
            CREATE OR REPLACE VIEW enr_dim_customers AS
            SELECT customer_id, company_name, city, country, region 
            FROM dwm_customers
            WHERE active = TRUE;

            CREATE OR REPLACE VIEW enr_dim_categories AS
            SELECT category_id, category_name 
            FROM dwm_categories
            WHERE active = TRUE;

            CREATE OR REPLACE VIEW enr_dim_suppliers AS
            SELECT supplier_id, company_name 
            FROM dwm_suppliers
            WHERE active = TRUE;

            CREATE OR REPLACE VIEW enr_dim_countries AS
            SELECT 
                -- 1. Identificadores
                country_name,           -- Business Key
                abbreviation,
                -- 2. Jerarquía Geográfica
                capital_city,
                latitude,
                longitude,
                -- 3. Datos de Mercado
                population,
                gdp,
                currency_code,
                official_language,
                unemployment_rate_percent AS unemployment_rate, 
                cpi
            FROM dwm_countries
            WHERE active = TRUE; -- Filtro vital para mostrar solo la versión vigente


        -- 3. VISTA ENRIQUECIDA DE VENTAS (FACTS)
        CREATE OR REPLACE VIEW enr_fact_table AS
        SELECT 
            od.product_id,          -- Business Key
            o.customer_id,          -- Business Key
            o.employee_id,          -- Business Key
            TO_CHAR(o.order_date, 'YYYYMMDD')::INTEGER AS date_key,
            od.quantity,
            od.unit_price,
            od.discount,
            CAST((od.unit_price * od.quantity) * (1 - od.discount) AS NUMERIC(12,2)) AS total_amount
        FROM dwm_order_details od
        JOIN dwm_orders o ON od.order_id = o.order_id
        -- IMPORTANTE: Filtramos ambas tablas para tomar solo la versión "oficial" (activa)
        WHERE od.active = TRUE 
        AND o.active = TRUE;


        -- ====================================================================
        -- LIMPIEZA (TRUNCATE) - ORDEN INVERSO PARA NO ROMPER FKs
        -- ====================================================================
        TRUNCATE TABLE fact_table RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dim_product RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dim_employee RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dim_customer RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dim_supplier RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dim_category RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dim_country RESTART IDENTITY CASCADE;

        -- ====================================================================
        --  CARGA DE DATOS EN DIMENSIONES Y FACTS
        -- ====================================================================


        -- 2.1 PAÍSES (Base de la geografía)
        INSERT INTO dim_country (
            country_name, abbreviation, capital_city, latitude, longitude,
            population, gdp, currency_code, official_language, unemployment_rate, cpi
        )
        SELECT 
            country_name, abbreviation, capital_city, latitude, longitude,
            population, gdp, currency_code, official_language, unemployment_rate, cpi
        FROM enr_dim_countries;

        -- 2.2 CATEGORÍAS (Base de productos)
	    INSERT INTO dim_category (category_id, category_name, load_date) 
	    SELECT category_id, category_name, NOW() FROM enr_dim_categories;



        -- 3.1 PROVEEDORES (Requiere Country_Key)
     INSERT INTO dim_supplier (supplier_id, company_name, load_date) 
    SELECT supplier_id, company_name, NOW() FROM enr_dim_suppliers;

        -- 3.2 CLIENTES (Requiere Country_Key)
   INSERT INTO dim_customer (customer_id, company_name, city, country, region, load_date)
    SELECT customer_id, company_name, city, country, region, NOW() FROM enr_dim_customers;

        -- 3.3 EMPLEADOS (Requiere Country_Key)
    INSERT INTO dim_employee (employee_id, full_name, title, city, country, region, hire_date, load_date)
    SELECT employee_id, full_name, title, city, country, region, hire_date, NOW() FROM enr_dim_employees;


        -- 4.1 PRODUCTOS (Requiere Category_Key y Supplier_Key)
     INSERT INTO dim_product (product_id, product_name, category_key, supplier_key, discontinued, load_date)
    SELECT 
        p.product_id, 
        p.product_name, 
        c.category_key, 
        s.supplier_key, 
        p.is_discontinued, -- Viene ya convertido a Boolean desde la vista ENR
        NOW()
    FROM enr_dim_products p
    LEFT JOIN dim_category c ON p.category_id = c.category_id
    LEFT JOIN dim_supplier s ON p.supplier_id = s.supplier_id;


   INSERT INTO fact_table (product_key, customer_key, employee_key, date_key, quantity, unit_price, discount, total_amount, load_date)
    SELECT 
        dp.product_key, 
        dc.customer_key, 
        de.employee_key, 
        dt.date_key, 
        f.quantity, 
        f.unit_price, 
        f.discount, 
        f.total_amount, -- Calculado en la vista ENR
        NOW()
    FROM enr_fact_sales f
    JOIN dim_product dp ON f.product_id = dp.product_id
    JOIN dim_customer dc ON f.customer_id = dc.customer_id
    JOIN dim_employee de ON f.employee_id = de.employee_id
    JOIN dim_time dt ON f.date_key = dt.date_key;



    -- =======================================================================    
    -- FIN DEL PROCESO
    -- =======================================================================    
-- Si esta sección finaliza sin error, actualiza el log como 'OK'
        UPDATE dqm_exec_log
        SET finished_at = NOW(),
            status = 'OK',
            message = 'Completado exitosamente.',
            rows_processed = 0 -- El valor acumulado
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