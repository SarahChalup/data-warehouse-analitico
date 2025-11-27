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
    v_script_name TEXT :=  'E2_03_enr_create_views.sql';
BEGIN
  -- Registrar el script en el inventario
  SELECT script_id INTO v_script_id 
  FROM dqm_scripts_inventory WHERE script_name = v_script_name;

  -- Si no existe (es NULL), lo creamos
  IF v_script_id IS NULL THEN
    INSERT INTO dqm_scripts_inventory (script_name, description, created_by, created_at)
    VALUES (
      v_script_name, 
      'Calcular datos derivados y crear vistas en la capa de enriquecimiento',
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
            -- CAPA DE ENRIQUECIMIENTO (ENR) - VISTAS LÓGICAS
            -- Prefijo sugerido: ENR_ (Enrichment Layer)
            -- Objetivo: Aplicar transformaciones y cálculos sin duplicar datos físicos.
            -- ===================================================================

            -- 1. VISTA ENRIQUECIDA DE PRODUCTOS
            -- Transformación: Convierte discontinued (0/1) a Booleano (True/False)
            CREATE OR REPLACE VIEW enr_dim_products AS
            SELECT 
                product_id,             -- Business Key
                product_name,
                supplier_id,            -- Se usará para el JOIN en la carga final
                category_id,            -- Se usará para el JOIN en la carga final
                quantity_per_unit,
                unit_price,
                -- REGLA DE LIMPIEZA/ESTANDARIZACIÓN
                CASE 
                    WHEN discontinued = 1 THEN TRUE 
                    ELSE FALSE 
                END AS is_discontinued
            FROM dwm_products;

            -- 2. VISTA ENRIQUECIDA DE EMPLEADOS
            -- Transformación: Concatenación de Nombre y Apellido
            CREATE OR REPLACE VIEW enr_dim_employees AS
            SELECT 
                employee_id,            -- Business Key
                -- DATO DERIVADO (Cálculo de texto)
                first_name || ' ' || last_name AS full_name,
                title,
                city,
                country,
                region,
                hire_date
            FROM dwm_employees;

            -- 3. VISTA ENRIQUECIDA DE VENTAS (FACTS)
            -- Transformación: Cálculo de Monto Total y Generación de Date_Key
            CREATE OR REPLACE VIEW enr_fact_sales AS
            SELECT 
                od.product_id,          -- Business Key (para buscar SK Producto)
                o.customer_id,          -- Business Key (para buscar SK Cliente)
                o.employee_id,          -- Business Key (para buscar SK Empleado)
                
                -- TRANSFORMACIÓN: Convertir fecha a entero YYYYMMDD para conectar con Dim_Time
                TO_CHAR(o.order_date, 'YYYYMMDD')::INTEGER AS date_key,
                
                od.quantity,
                od.unit_price,
                od.discount,
                
                -- DATO DERIVADO (Cálculo Matemático): (Precio * Cantidad) - Descuento
                CAST((od.unit_price * od.quantity) * (1 - od.discount) AS NUMERIC(12,2)) AS total_amount

            FROM dwm_order_details od
            JOIN dwm_orders o ON od.order_id = o.order_id; -- Unimos para obtener fechas y clientes

            -- 4. VISTAS DE PASO DIRECTO (Passthrough)
            -- Aunque no tengan lógica compleja, es buena práctica crear vistas para
            -- mantener la consistencia: DWM -> ENR -> DWA

            CREATE OR REPLACE VIEW enr_dim_customers AS
            SELECT customer_id, company_name, city, country, region FROM dwm_customers;

            CREATE OR REPLACE VIEW enr_dim_categories AS
            SELECT category_id, category_name FROM dwm_categories;

            CREATE OR REPLACE VIEW enr_dim_suppliers AS
            SELECT supplier_id, company_name FROM dwm_suppliers;







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