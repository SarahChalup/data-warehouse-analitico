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
    v_script_name TEXT :=  'E2_05_dwa_carga_enriquecimiento.sql';
BEGIN
  -- Registrar el script en el inventario
  SELECT script_id INTO v_script_id 
  FROM dqm_scripts_inventory WHERE script_name = v_script_name;

  -- Si no existe (es NULL), lo creamos
  IF v_script_id IS NULL THEN
    INSERT INTO dqm_scripts_inventory (script_name, description, created_by, created_at)
    VALUES (
      v_script_name, 
      'Calcular datos derivados y cargar datos en la capa de enriquecimiento',
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


-- ===================================================================
-- PROCESO ETL: CARGA Y ENRIQUECIMIENTO (DWM -> DIM/FACT)
-- ===================================================================

-- -------------------------------------------------------------------
-- PASO 1: GENERACIÓN DE LA DIMENSIÓN TIEMPO (Enriquecimiento Puro)
-- -------------------------------------------------------------------
-- No viene de una tabla origen, generamos fechas desde 1996 (Northwind) hasta 2030
TRUNCATE TABLE dim_time CASCADE;

INSERT INTO dim_time (date_key, full_date, year, month, quarter, day, is_weekend)
SELECT
    TO_CHAR(datum, 'YYYYMMDD')::INTEGER AS date_key,
    datum AS full_date,
    EXTRACT(YEAR FROM datum) AS year,
    EXTRACT(MONTH FROM datum) AS month,
    'Q' || EXTRACT(QUARTER FROM datum) AS quarter,
    EXTRACT(DAY FROM datum) AS day,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    -- Generamos una secuencia de días por 35 años
    SELECT '1996-01-01'::DATE + sequence.day AS datum
    FROM generate_series(0, 12000) AS sequence(day)
) DQ
ORDER BY 1;


-- -------------------------------------------------------------------
-- PASO 2: DIMENSIONES EXTERNAS (Puntas del Copo de Nieve)
-- -------------------------------------------------------------------

-- Carga Dimensión Categoría
TRUNCATE TABLE dim_category CASCADE;
INSERT INTO dim_category (category_id, category_name)
SELECT category_id, category_name
FROM dwm_categories;

-- Carga Dimensión Proveedor
TRUNCATE TABLE dim_supplier CASCADE;
INSERT INTO dim_supplier (supplier_id, company_name)
SELECT supplier_id, company_name
FROM dwm_suppliers;


-- -------------------------------------------------------------------
-- PASO 3: DIMENSIÓN PRODUCTO (Requiere JOIN con Category y Supplier)
-- -------------------------------------------------------------------
-- Aquí ocurre la magia del Snowflake: Buscamos las KEYS de las otras dimensiones
TRUNCATE TABLE dim_product CASCADE;

INSERT INTO dim_product (product_id, product_name, category_key, supplier_key, discontinued)
SELECT 
    p.product_id,
    p.product_name,
    c.category_key,   -- Traemos la SK de Categoría
    s.supplier_key,   -- Traemos la SK de Proveedor
    CASE WHEN p.discontinued = 1 THEN TRUE ELSE FALSE END -- Transformación de dato (Int a Bool)
FROM dwm_products p
-- Hacemos LEFT JOIN para asegurar que no perdemos productos si falta categoría/proveedor
LEFT JOIN dim_category c ON p.category_id = c.category_id
LEFT JOIN dim_supplier s ON p.supplier_id = s.supplier_id;


-- -------------------------------------------------------------------
-- PASO 4: DIMENSIONES INDEPENDIENTES
-- -------------------------------------------------------------------

-- Carga Dimensión Cliente
TRUNCATE TABLE dim_customer CASCADE;
INSERT INTO dim_customer (customer_id, company_name, city, country, region)
SELECT customer_id, company_name, city, country, region
FROM dwm_customers;

-- Carga Dimensión Empleado
-- Enriquecimiento: Concatenamos Nombre y Apellido
TRUNCATE TABLE dim_employee CASCADE;
INSERT INTO dim_employee (employee_id, full_name, title, city, country, region, hire_date)
SELECT 
    employee_id,
    first_name || ' ' || last_name, -- Concatenación
    title, 
    city, 
    country, 
    region, 
    hire_date
FROM dwm_employees;


-- -------------------------------------------------------------------
-- PASO 5: TABLA DE HECHOS (FACT_TABLE) - EL GRAN CRUCE
-- -------------------------------------------------------------------
TRUNCATE TABLE fact_table CASCADE;

INSERT INTO fact_table (
    product_key, 
    customer_key, 
    employee_key, 
    date_key, 
    quantity, 
    unit_price, 
    discount, 
    total_amount
)
SELECT 
    dp.product_key,
    dc.customer_key,
    de.employee_key,
    dt.date_key,
    od.quantity,
    od.unit_price,
    od.discount,
    -- CÁLCULO DE ENRIQUECIMIENTO (Monto Total Neto)
    CAST((od.unit_price * od.quantity) * (1 - od.discount) AS NUMERIC(12,2))
FROM dwm_order_details od
-- 1. Unimos Detalle con Cabecera para tener fechas y clientes
JOIN dwm_orders o ON od.order_id = o.order_id
-- 2. Buscamos la Key de PRODUCTO
JOIN dim_product dp ON od.product_id = dp.product_id
-- 3. Buscamos la Key de CLIENTE
JOIN dim_customer dc ON o.customer_id = dc.customer_id
-- 4. Buscamos la Key de EMPLEADO
JOIN dim_employee de ON o.employee_id = de.employee_id
-- 5. Buscamos la Key de TIEMPO (Usando la fecha de la orden)
JOIN dim_time dt ON TO_CHAR(o.order_date, 'YYYYMMDD')::INTEGER = dt.date_key;







-------------------------------------------------------------------
    -- ACTUALIZAR CONTADOR DE FILAS PROCESADAS (MODELO DIMENSIONAL)
    -------------------------------------------------------------------
    SELECT 
        (SELECT COUNT(*) FROM dim_category) +
        (SELECT COUNT(*) FROM dim_supplier) +
        (SELECT COUNT(*) FROM dim_product) +
        (SELECT COUNT(*) FROM dim_customer) +
        (SELECT COUNT(*) FROM dim_employee) +
        (SELECT COUNT(*) FROM dim_time) +
        (SELECT COUNT(*) FROM fact_table)
    INTO v_total_processed_rows; -- Ojo: Aquí podrías sumar al valor anterior o sobreescribirlo según tu lógica



    -- =======================================================================    
    -- FIN DEL PROCESO
    -- =======================================================================    
-- Si esta sección finaliza sin error, actualiza el log como 'OK'
        UPDATE dqm_exec_log
        SET finished_at = NOW(),
            status = 'OK',
            message = 'Completado exitosamente.',
            rows_processed = v_total_processed_rows -- El valor acumulado
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