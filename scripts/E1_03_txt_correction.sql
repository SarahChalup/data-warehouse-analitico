DO $$
DECLARE
  v_log_id BIGINT;
  v_script_id INT; 
  -- Contadore de filas procesadas
  v_total_rows BIGINT := 0;
  v_current_table_rows BIGINT;
	-- Control de errores
  v_msg text;
  v_detail text;
  v_context text;
  v_script_name TEXT := 'E1_03_txt_correction.sql';
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



--CORRIGIENDO ERRORES DE VALIDACIÓN


-- TXT_CUSTOMERS
	update txt_customers 
	set regions = null
	where regions = 'NULL';

    GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	v_total_rows = v_total_rows + v_current_table_rows;

    update txt_customers 
	set postal_code = null
	where postal_code = 'NULL';
    
    GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	v_total_rows = v_total_rows + v_current_table_rows;

    update txt_customers 
	set fax = null
	where fax = 'NULL';
    
    GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	v_total_rows = v_total_rows + v_current_table_rows;
  
    
-- TXT_CUSTOMERS
	update txt_employees 
	set region = null
	where region = 'NULL';

    GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	v_total_rows = v_total_rows + v_current_table_rows;

-- TXT_SUPPLIERS
	update txt_suppliers 
	set region = null
	where region = 'NULL';

    GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	v_total_rows = v_total_rows + v_current_table_rows;

    update txt_suppliers 
	set fax = null
	where fax = 'NULL';

    GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	v_total_rows = v_total_rows + v_current_table_rows;
    
    update txt_orders 
	set ship_region = null
	where ship_region = 'NULL';

    GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	v_total_rows = v_total_rows + v_current_table_rows;
    
    update txt_orders 
	set ship_postal_code = null
	where ship_postal_code = 'NULL';

    GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	v_total_rows = v_total_rows + v_current_table_rows;
    
    update txt_orders 
	set shipped_date = null
	where shipped_date = 'NULL';

    GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
	v_total_rows = v_total_rows + v_current_table_rows;
      


-- ==========================================================
    -- 4. FINALIZAR EL LOG SEGÚN RESULTADOS DE VALIDACIÓN
-- ==========================================================
 
            -- Terminamos limpio
            UPDATE dqm_exec_log
            SET finished_at = NOW(),
                status = 'OK',
                message = 'Corrección exitosa.',
                rows_processed = v_total_rows
            WHERE log_id = v_log_id;
            
            RAISE NOTICE 'Script finalizado exitosamente sin errores.';
  

    -- ==================================================================
    -- 5. MANEJO DE FALLOS TÉCNICOS (Sintaxis, Conexión, Tablas no existen)
    -- ==================================================================
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
