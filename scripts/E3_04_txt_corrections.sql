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
    v_script_name TEXT :=  'E3_04_txt_corrections.sql';
BEGIN
  -- Registrar el script en el inventario
  SELECT script_id INTO v_script_id 
  FROM dqm_scripts_inventory WHERE script_name = v_script_name;

  -- Si no existe (es NULL), lo creamos
  IF v_script_id IS NULL THEN
    INSERT INTO dqm_scripts_inventory (script_name, description, created_by, created_at)
    VALUES (
      v_script_name, 
      'Corregir errores de formato de ingesta 2',
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

            update txt_customers 
            set regions = null
            where regions = 'NULL';

            GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
            v_total_rows = v_total_rows + v_current_table_rows;
 
            update txt_orders 
            set ship_region = null
            where ship_region = 'NULL';

            GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
            v_total_rows = v_total_rows + v_current_table_rows;
 
            
            update txt_countries tc  
            set armed_forces_size = null
            where armed_forces_size = '';

              GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
            v_total_rows = v_total_rows + v_current_table_rows;
          
            
            update txt_countries tc  
            set armed_forces_size = null
            where armed_forces_size = 'NULL';

             GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
            v_total_rows = v_total_rows + v_current_table_rows;
           
            
            UPDATE txt_countries
            SET 
            population = REPLACE(population, ',', ''),
            urban_population = REPLACE(urban_population, ',', ''),
            gdp = REPLACE(gdp, ',', ''),
            land_area_km2 = REPLACE(land_area_km2, ',', ''),
            armed_forces_size = REPLACE(armed_forces_size, ',', ''),
            co2_emissions = REPLACE(co2_emissions, ',', ''),
            cpi = REPLACE(armed_forces_size, ',', '');

                GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
            v_total_rows = v_total_rows + v_current_table_rows;
        
            
            UPDATE txt_countries
            SET 
            agricultural_land_percent = REPLACE(agricultural_land_percent, '%', ''),
            forested_area_percent = REPLACE(forested_area_percent, '%', ''),
            cpi_change_percent = REPLACE(cpi_change_percent, '%', ''),
            gross_primary_education_enrollment_percent = REPLACE(gross_primary_education_enrollment_percent, '%', ''),
            gross_tertiary_education_enrollment_percent = REPLACE(gross_tertiary_education_enrollment_percent, '%', ''),
            out_of_pocket_health_expenditure_percent = REPLACE(out_of_pocket_health_expenditure_percent, '%', ''),
            labor_force_participation_percent = REPLACE(labor_force_participation_percent, '%', ''),
            tax_revenue_percent = REPLACE(tax_revenue_percent, '%', ''),
            total_tax_rate_percent = REPLACE(total_tax_rate_percent, '%', ''),
            unemployment_rate_percent = REPLACE(unemployment_rate_percent, '%', '');

            GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
            v_total_rows = v_total_rows + v_current_table_rows;
      
            
            UPDATE txt_countries
        SET 
            gasoline_price = TRIM(REPLACE(gasoline_price, '$', '')),
            gdp = TRIM(REPLACE(gdp, '$', '')),
            minimum_wage = TRIM(REPLACE(minimum_wage, '$', ''))
        ;
        
           GET DIAGNOSTICS v_current_table_rows = ROW_COUNT;
            v_total_rows = v_total_rows + v_current_table_rows;
 

            UPDATE txt_countries
            SET 
            life_expectancy = TRIM(REPLACE(life_expectancy, '', NULL)),
            maternal_mortality_ratio  = TRIM(REPLACE(maternal_mortality_ratio, '', NULL)),
            labor_force_participation_percent = TRIM(REPLACE(labor_force_participation_percent, '', NULL)),
            tax_revenue_percent = TRIM(REPLACE(tax_revenue_percent, '', NULL)),
            total_tax_rate_percent= TRIM(REPLACE(total_tax_rate_percent, '', NULL)),
            unemployment_rate_percent = TRIM(REPLACE(unemployment_rate_percent, '', NULL)),
            agricultural_land_percent = TRIM(REPLACE(unemployment_rate_percent, '', NULL)),
            forested_area_percent = TRIM(REPLACE(unemployment_rate_percent, '', NULL)),

            minimum_wage = TRIM(REPLACE(unemployment_rate_percent, '', NULL))

            ;


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

