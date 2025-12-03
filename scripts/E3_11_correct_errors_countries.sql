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
    v_script_name TEXT :=  'E3_11_correct_errors_countries.sql';
BEGIN
  -- Registrar el script en el inventario
  SELECT script_id INTO v_script_id 
  FROM dqm_scripts_inventory WHERE script_name = v_script_name;

  -- Si no existe (es NULL), lo creamos
  IF v_script_id IS NULL THEN
    INSERT INTO dqm_scripts_inventory (script_name, description, created_by, created_at)
    VALUES (
      v_script_name, 
      'Corregir errores de integridad de paises',
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



     UPDATE dwm_customers
    SET country = CASE 
        WHEN country = 'UK' THEN 'United Kingdom'
        WHEN country = 'USA' THEN 'United States'
        WHEN country = 'Ireland' THEN 'Republic of Ireland'
    END
    WHERE country IN ('UK', 'USA', 'Ireland');


UPDATE dwm_employees
SET country = CASE 
    WHEN country = 'UK' THEN 'United Kingdom'
    WHEN country = 'USA' THEN 'United States'
    WHEN country = 'Ireland' THEN 'Republic of Ireland'
END
WHERE country IN ('UK', 'USA', 'Ireland');


-- 418: Libya (LY) -> Capital Trípoli
    UPDATE dwm_countries 
    SET capital_city = 'Tripoli' 
    WHERE dwm_id = 418;

    -- 419: Singapore (SG) -> Capital Singapore
    UPDATE dwm_countries 
    SET capital_city = 'Singapore' 
    WHERE dwm_id = 419;

    -- 440: Costa Rica (CR) -> Capital San José (Corregido a San Jose)
    UPDATE dwm_countries 
    SET capital_city = 'San Jose' 
    WHERE dwm_id = 440;

    -- 444: Palestinian National Authority -> Código PS, Capital Ramallah
    UPDATE dwm_countries 
    SET abbreviation = 'PS', 
        capital_city = 'Ramallah' 
    WHERE dwm_id = 444;

    -- 451: Brazil (BR) -> Capital Brasilia
    UPDATE dwm_countries 
    SET capital_city = 'Brasilia' 
    WHERE dwm_id = 451;

    -- 474: S... (ST) -> Sao Tome and Principe, Capital Sao Tome
    UPDATE dwm_countries 
    SET country_name = 'Sao Tome and Principe', 
        capital_city = 'Sao Tome' 
    WHERE dwm_id = 474;

    -- 480: Togo (TG) -> Capital Lomé (Corregido a Lome)
    UPDATE dwm_countries 
    SET capital_city = 'Lome' 
    WHERE dwm_id = 480;

    -- 494: Tonga (TO) -> Capital Nuku'alofa (Corregido escaping la comilla simple)
    UPDATE dwm_countries 
    SET capital_city = 'Nuku''alofa' 
    WHERE dwm_id = 494;

    -- 508: Iceland (IS) -> Capital Reykjavík (Corregido a Reykjavik)
    UPDATE dwm_countries 
    SET capital_city = 'Reykjavik' 
    WHERE dwm_id = 508;

    -- 544: Cameroon (CM) -> Capital Yaoundé (Corregido a Yaounde)
    UPDATE dwm_countries 
    SET capital_city = 'Yaounde' 
    WHERE dwm_id = 544;

    -- 557: Paraguay (PY) -> Capital Asunción (Corregido a Asuncion)
    UPDATE dwm_countries 
    SET capital_city = 'Asuncion' 
    WHERE dwm_id = 557;

    -- 563: Colombia (CO) -> Capital Bogotá (Corregido a Bogota)
    UPDATE dwm_countries 
    SET capital_city = 'Bogota' 
    WHERE dwm_id = 563;

    -- 568: Maldives (MV) -> Capital Malé (Corregido a Male)
    UPDATE dwm_countries 
    SET capital_city = 'Male' 
    WHERE dwm_id = 568;

    -- 579: Moldova (MD) -> Capital Chișinău (Corregido a Chisinau)
    UPDATE dwm_countries 
    SET capital_city = 'Chisinau' 
    WHERE dwm_id = 579;








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




-------------------------------------------------
-- scripts de deteccion de errores

-- Busca en CUSTOMERS
SELECT 
    'dwm_customers' AS tabla_origen,
    customer_id::TEXT AS id_registro,
    country AS valor_problematico
FROM dwm_customers c
WHERE c.country IS NOT NULL 
  AND TRIM(c.country) <> ''
  AND NOT EXISTS (
      SELECT 1 
      FROM dwm_countries co 
      WHERE LOWER(TRIM(co.country_name)) = LOWER(TRIM(c.country))
  )

UNION ALL

-- Busca en SUPPLIERS
SELECT 
    'dwm_suppliers',
    supplier_id::TEXT,
    country
FROM dwm_suppliers s
WHERE s.country IS NOT NULL 
  AND TRIM(s.country) <> ''
  AND NOT EXISTS (
      SELECT 1 
      FROM dwm_countries co 
      WHERE LOWER(TRIM(co.country_name)) = LOWER(TRIM(s.country))
  )

UNION ALL

-- Busca en EMPLOYEES
SELECT 
    'dwm_employees',
    employee_id::TEXT,
    country
FROM dwm_employees e
WHERE e.country IS NOT NULL 
  AND TRIM(e.country) <> ''
  AND NOT EXISTS (
      SELECT 1 
      FROM dwm_countries co 
      WHERE LOWER(TRIM(co.country_name)) = LOWER(TRIM(e.country))
  )

ORDER BY 1, 3;


--BUSCANDO PAISES REPETIDOS EN DWM_COUNTRIES
SELECT 
    LOWER(TRIM(country_name)) as nombre_normalizado,
    COUNT(*) as cantidad_repetida,
    STRING_AGG(dwm_id::TEXT, ', ') as ids_implicados,
    STRING_AGG(country_name, ' | ') as variaciones_nombre
FROM dwm_countries
GROUP BY LOWER(TRIM(country_name))
HAVING COUNT(*) > 1;

-- BUSCANDO VALORES NO ASCII EN DWM_COUNTRIES
SELECT 
    dwm_id, 
    country_name, 
    capital_city,
    abbreviation
FROM dwm_countries
WHERE country_name !~ '^[\x20-\x7E]+$' -- Busca cosas fuera del rango ASCII imprimible
   OR capital_city !~ '^[\x20-\x7E]+$';

