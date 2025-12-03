DO $$
DECLARE
    v_log_id BIGINT;
    v_script_id INT := 1;  
    v_msg text;
    v_detail text;
    v_script_name TEXT :=  'E4_01_dwa_data_product.sql';
BEGIN
  -- Registrar el script en el inventario
  SELECT script_id INTO v_script_id 
  FROM dqm_scripts_inventory WHERE script_name = v_script_name;

  -- Si no existe (es NULL), lo creamos
  IF v_script_id IS NULL THEN
    INSERT INTO dqm_scripts_inventory (script_name, description, created_by, created_at)
    VALUES (
      v_script_name, 
      'Crear vista para producto de datos DWA',
      'Equipo DWA',
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

    

      CREATE OR REPLACE VIEW vw_sales_by_employee AS
      SELECT 
          DE.employee_key,
          DE.full_name,
          DE.country,     
          DT."year",
          DT.quarter,
          
          -- Métricas
          SUM(FT.total_amount) as total_revenue,
          COUNT(ft.fact_key) AS total_orders,
          
          -- Agregado: Cálculo del valor promedio (evitando división por 0)
          CASE 
              WHEN COUNT(ft.fact_key) > 0 THEN 
                  ROUND(SUM(FT.total_amount) / COUNT(ft.fact_key), 2)
              ELSE 0 
          END AS average_order_value,
          
          SUM(FT.quantity) AS total_units_sold

      FROM fact_table ft 
      JOIN dim_employee de ON DE.employee_key = FT.employee_key 
      JOIN dim_time dt ON DT.date_key = FT.date_key 
      GROUP BY 
          DE.employee_key,
          DE.full_name,
          DE.country,
          DT."year",
          DT.quarter;


-- Agregar la vista a la metadata

      INSERT INTO md_entities (
            entity_name,
            business_name,
            layer,
            entity_type,
            grain,
            primary_key,
            description,
            created_by
        )
        VALUES (
            'vw_sales_by_employee',
            'Vista para análisis de ventas por empleado',
            'DWA_PROD',
            'VIEW',
            'Una fila por empleado por período de tiempo',
            'employee_key, year, quarter',
            'Vista que agrega ventas por empleado y período de tiempo para análisis de desempeño.',
            'Sarah'
        );


    -- ==========================================================
    -- 3. MANEJO DE EXCEPCIONES
    -- ==========================================================
    EXCEPTION
        WHEN OTHERS THEN
-- Capturar diagnóstico completo
            GET STACKED DIAGNOSTICS 
                v_msg = MESSAGE_TEXT, 
                v_detail = PG_EXCEPTION_DETAIL;
            
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