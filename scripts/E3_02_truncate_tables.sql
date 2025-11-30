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
    v_script_name TEXT :=  'E3_02_tmp_clean.sql';
BEGIN
  -- Registrar el script en el inventario
  SELECT script_id INTO v_script_id 
  FROM dqm_scripts_inventory WHERE script_name = v_script_name;

  -- Si no existe (es NULL), lo creamos
  IF v_script_id IS NULL THEN
    INSERT INTO dqm_scripts_inventory (script_name, description, created_by, created_at)
    VALUES (
      v_script_name, 
      'Limpiar tablas txt y tmp. Agregar campo active a tablas dwm',
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

    -- =======================================================================
    -- Limpieza de tablas txt

    TRUNCATE TABLE txt_categories;
    TRUNCATE TABLE txt_customers;
    TRUNCATE TABLE txt_employees;
    TRUNCATE TABLE txt_orders;
    TRUNCATE TABLE txt_order_details;
    TRUNCATE TABLE txt_products;
    TRUNCATE TABLE txt_suppliers;
    truncate table txt_shippers;
    truncate table txt_territories;
    truncate table txt_employees_territories;
    truncate table txt_regions;

    -- =======================================================================
    -- Limpeza de tablas tmp
    TRUNCATE TABLE tmp_categories;
    TRUNCATE TABLE tmp_customers;
    TRUNCATE TABLE tmp_employees;
    TRUNCATE TABLE tmp_orders;
    TRUNCATE TABLE tmp_order_details;
    TRUNCATE TABLE tmp_products;
    TRUNCATE TABLE tmp_suppliers;
    truncate table tmp_shippers;
    truncate table tmp_territories;
    truncate table tmp_employees_territories;
    truncate table tmp_regions;

    -- =======================================================================
    -- Agregar campo active a tablas dwm si no existe

    alter table dwm_customers
    add column if not exists active boolean default true;

    alter table dwm_suppliers
    add column if not exists active boolean default true;

    alter table dwm_products
    add column if not exists active boolean default true;

    alter table dwm_employees
    add column if not exists active boolean default true;

    alter table dwm_shippers
    add column if not exists active boolean default true;

    alter table dwm_categories
    add column if not exists active boolean default true;

    alter table dwm_territories
    add column if not exists active boolean default true;

    alter table dwm_regions
    add column if not exists active boolean default true;

    alter table dwm_orders
    add column if not exists active boolean default true;

    alter table dwm_order_details
    add column if not exists active boolean default true;

    alter table dwm_employees_territories
    add column if not exists active boolean default true;



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