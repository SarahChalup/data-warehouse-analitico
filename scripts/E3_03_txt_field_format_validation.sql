DO $$
DECLARE
  v_log_id BIGINT;
  v_script_id INT; 
  -- Contadore de filas procesadas
  v_total_validated_rows BIGINT := 0;
  v_current_table_rows BIGINT;
  --Contador de errores encontrados
  v_errors_count INT := 0; 
	-- Control de errores
  v_msg text;
  v_detail text;
  v_context text;
  v_script_name TEXT := 'E3_03_txt_field_format_validation.sql';
BEGIN

  -- Registrar el script en el inventario
  SELECT script_id INTO v_script_id 
  FROM dqm_scripts_inventory WHERE script_name = v_script_name;

  -- Si no existe (es NULL), lo creamos
  IF v_script_id IS NULL THEN
    INSERT INTO dqm_scripts_inventory (script_name, description, created_by, created_at)
    VALUES (
      v_script_name, 
      'Validar formato de campos de tablas txt para la etapa de Adquisición',
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

      ------------------------------------------------------------
      ------------------------------------------------------------
      -- 3. Procesos de validacion

      -- 


      -- TXT_CATEGORIES TABLE -----------------------------------

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_categories', category_id, 'category_id no es numérico'
        FROM txt_categories
        WHERE category_id !~ '^[0-9]+$'
        OR category_id = 'NULL'
        OR category_id = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_categories', category_id, 'category_name excede 50 caracteres'
        FROM txt_categories
        WHERE length(category_name) > 50
        OR category_name = 'NULL'
        OR category_name = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_categories', category_id, 'picture no es un valor hexadecimal válido'
        FROM txt_categories
        WHERE (picture IS NOT NULL
        AND decode(replace(picture, '0x', ''), 'hex') IS NULL)
        OR picture = 'NULL'
        OR picture = '';

        -- 4. Registro de reglas en field check
        INSERT INTO dqm_field_checks (table_name, column_name, rule_type, rule_param, description)
        VALUES 
        ('txt_categories', 'category_id', 'TYPE', 'INTEGER', 'Debe ser entero'),
        ('txt_categories', 'category_name', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_categories', 'description', 'TYPE', 'TEXT', 'Debe ser texto'),
        ('txt_categories', 'picture', 'TYPE', 'BYTEA', 'Debe ser un valor hexadecimal valido');

        -- 5. Contador de filas validadas en la tabla actual
        EXECUTE 'SELECT COUNT(*) FROM txt_categories' INTO v_current_table_rows;
        v_total_validated_rows := v_total_validated_rows + v_current_table_rows;


      -- TXT_CUSTOMERS TABLE -----------------------------------

        
        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_customers', customer_id, 'customer_id excede 10 caracteres'
        FROM txt_customers
        WHERE length(customer_id) > 10
        OR customer_id = 'NULL'
        OR customer_id = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_customers', customer_id, 'customer_name excede 100 caracteres'
        FROM txt_customers
        WHERE length(customer_name) > 100
        OR customer_name = 'NULL'
        OR customer_name = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_customers', customer_id, 'contact_name excede 100 caracteres'
        FROM txt_customers
        WHERE length(contact_name) > 100
        OR contact_name = 'NULL'
        OR contact_name = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_customers', customer_id, 'contact_title excede 100 caracteres'
        FROM txt_customers
        WHERE length(contact_title) > 100
        OR contact_title = 'NULL'
        OR contact_title = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_customers', customer_id, 'address excede 200 caracteres'
        FROM txt_customers
        WHERE length(address) > 200
        OR address = 'NULL'
        OR address = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_customers', customer_id, 'city excede 50 caracteres'
        FROM txt_customers
        WHERE length(city) > 50
        OR city = 'NULL'
        OR city = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_customers', customer_id, 'regions excede 50 caracteres'
        FROM txt_customers
        WHERE length(regions) > 50
        OR regions = 'NULL'
        OR regions = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_customers', customer_id, 'postal_code excede 20 caracteres'
        FROM txt_customers
        WHERE length(postal_code) > 20
        OR postal_code = 'NULL'
        OR postal_code = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_customers', customer_id, 'country excede 50 caracteres'
        FROM txt_customers
        WHERE length(country) > 50
        OR country = 'NULL'
        OR country = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_customers', customer_id, 'phone excede 50 caracteres'
        FROM txt_customers
        WHERE length(phone) > 50
        OR phone = 'NULL'
        OR phone = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_customers', customer_id, 'fax excede 50 caracteres'
        FROM txt_customers
        WHERE length(fax) > 50
        OR fax = 'NULL'
        OR fax = '';

        -- Registro de reglas en field check
        INSERT INTO dqm_field_checks (table_name, column_name, rule_type, rule_param, description)
        VALUES 
        ('txt_customers', 'customer_id', 'LENGTH', '10', 'Max 10 chars'),
        ('txt_customers', 'customer_name', 'LENGTH', '10', 'Max 10 chars'),
        ('txt_customers', 'contact_name', 'LENGTH', '10', 'Max 10 chars'),
        ('txt_customers', 'contact_title', 'LENGTH', '10', 'Max 10 chars'),
        ('txt_customers', 'address', 'LENGTH', '200', 'Max 200 chars'),
        ('txt_customers', 'city', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_customers', 'regions', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_customers', 'postal_code', 'LENGTH', '20', 'Max 20 chars'),
        ('txt_customers', 'country', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_customers', 'phone', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_customers', 'fax', 'LENGTH', '50', 'Max 50 chars');

      -- Contador de filas validadas en la tabla actual
          EXECUTE 'SELECT COUNT(*) FROM txt_customers' INTO v_current_table_rows;
          v_total_validated_rows := v_total_validated_rows + v_current_table_rows;




      -- TXT_EMPLOYEES_TERRITORIES TABLE -----------------------------------

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employee_territories', employee_id, 'employee_id no es numérico'
        FROM txt_employee_territories
        WHERE employee_id !~ '^[0-9]+$'
        OR employee_id = 'NULL'
        OR employee_id = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employee_territories', employee_id, 'territory_id excede 20 caracteres'
        FROM txt_employee_territories
        WHERE LENGTH(territory_id) > 20
        OR territory_id = 'NULL'
        OR territory_id = '';


     
        -- Registro de reglas en field check
        INSERT INTO dqm_field_checks (table_name, column_name, rule_type, rule_param, description)
        VALUES 
        ('txt_employee_territories', 'employee_id', 'TYPE', 'INTEGER', 'Debe ser entero'),
        ('txt_employee_territories', 'territory_id', 'LENGTH', '20', 'Max 20 chars');

        -- Contador de filas validadas en la tabla actual
            EXECUTE 'SELECT COUNT(*) FROM txt_employee_territories' INTO v_current_table_rows;
            v_total_validated_rows := v_total_validated_rows + v_current_table_rows;



      -- TXT_EMPLOYEES TABLE -----------------------------------
        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'employee_id no es numérico'
        FROM txt_employees
        WHERE employee_id !~ '^[0-9]+$'
        OR employee_id = 'NULL'
        OR employee_id = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'last_name excede 50 caracteres'
        FROM txt_employees
        WHERE LENGTH(last_name) > 50
        OR last_name = 'NULL'
        OR last_name = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'first_name excede 50 caracteres'
        FROM txt_employees
        WHERE LENGTH(first_name) > 50
        OR first_name = 'NULL'
        OR first_name = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'title excede 100 caracteres'
        FROM txt_employees
        WHERE LENGTH(title) > 100
        OR title = 'NULL'
        OR title = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'title_of_courtesy excede 50 caracteres'
        FROM txt_employees
        WHERE LENGTH(title_of_courtesy) > 50
        OR title_of_courtesy = 'NULL'
        OR title_of_courtesy = '';

      -- Validar formato de fecha en birth_date
        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'birth_date formato inválido'
        FROM txt_employees
        WHERE (birth_date IS NOT NULL
        AND birth_date !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$')
        OR birth_date = 'NULL'
        OR birth_date = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'hire_date formato inválido'
        FROM txt_employees
        WHERE (hire_date IS NOT NULL
        AND hire_date !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$')
        OR hire_date = 'NULL'
        OR hire_date = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'address excede 200 caracteres'
        FROM txt_employees
        WHERE LENGTH(address) > 200
         OR address = 'NULL'
        OR address = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'city excede 50 caracteres'
        FROM txt_employees
        WHERE LENGTH(city) > 50
        OR city = 'NULL'
        OR city = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'region excede 50 caracteres'
        FROM txt_employees
        WHERE LENGTH(region) > 50
        OR region = 'NULL'
        OR region = ''  ;

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'postal_code excede 20 caracteres'
        FROM txt_employees
        WHERE LENGTH(postal_code) > 20
        OR postal_code = 'NULL'
        OR postal_code = '';  

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'country excede 50 caracteres'
        FROM txt_employees
        WHERE LENGTH(country) > 50
        OR country = 'NULL'
        OR COUNTRY = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'home_phone excede 50 caracteres'
        FROM txt_employees
        WHERE LENGTH(home_phone) > 50
        OR home_phone = 'NULL'
        OR home_phone = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'extension excede 10 caracteres'
        FROM txt_employees
        WHERE LENGTH(extension) > 10
        OR extension = 'NULL'
        OR extension = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'photo no es un valor hexadecimal válido'
        FROM txt_employees
        WHERE (photo IS NOT NULL
        AND decode(replace(photo, '0x', ''), 'hex') IS NULL)
        OR photo = 'NULL'
        OR photo = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'reports_to no es numérico'
        FROM txt_employees
        WHERE reports_to !~ '^[0-9]+$'
        OR reports_to = 'NULL'
        OR reports_to = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_employees', employee_id, 'photo_path excede 200 caracteres'
        FROM txt_employees
        WHERE LENGTH(photo_path) > 200
        OR photo_path = 'NULL'
        OR photo_path = '';

      -- Registro de reglas en field check
        INSERT INTO dqm_field_checks (table_name, column_name, rule_type, rule_param, description)
        VALUES 
        ('txt_employees', 'employee_id', 'TYPE', 'INTEGER', 'Debe ser entero'),
        ('txt_employees', 'last_name', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_employees', 'first_name', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_employees', 'title', 'LENGTH', '100', 'Max 100 chars'),
        ('txt_employees', 'title_of_courtesy', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_employees', 'birth_date', 'FORMAT', 'YYYY-MM-DD HH24:MI:SS.MS', 'Formato de fecha invalido'),
        ('txt_employees', 'hire_date', 'FORMAT', 'YYYY-MM-DD HH24:MI:SS.MS', 'Formato de fecha invalido'),
        ('txt_employees', 'address', 'LENGTH', '200', 'Max 200 chars'),
        ('txt_employees', 'city', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_employees', 'region', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_employees', 'postal_code', 'LENGTH', '20', 'Max 20 chars'),
        ('txt_employees', 'country', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_employees', 'home_phone', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_employees', 'extension', 'LENGTH', '10',  'Max 10 chars'),
        ('txt_employees', 'photo', 'TYPE', 'BYTEA',  'Debe ser un valor hexadecimal valido'),
        ('txt_employees', 'notes', 'TYPE',  'TEXT',  'Debe ser texto'),
        ('txt_employees', 'reports_to', 'TYPE',  'INTEGER',  'Debe ser entero'),
        ('txt_employees', 'photo_path', 'LENGTH',  '200',  'Max 200 chars');

        -- Contador de filas validadas en la tabla actual
          EXECUTE 'SELECT COUNT(*) FROM txt_employees' INTO v_current_table_rows;
          v_total_validated_rows := v_total_validated_rows + v_current_table_rows;




      -- TXT_SHIPPERS TABLE -----------------------------------

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_shippers', shipper_id, 'shipper_id no es numérico'
        FROM txt_shippers
        WHERE shipper_id !~ '^[0-9]+$'
        OR shipper_id = 'NULL'
        OR shipper_id = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_shippers', shipper_id, 'company_name excede 100 caracteres'
        FROM txt_shippers
        WHERE LENGTH(company_name) > 100
        OR company_name = 'NULL'
        OR company_name = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_shippers', shipper_id, 'phone excede 50 caracteres'
        FROM txt_shippers
        WHERE LENGTH(phone) > 50
        OR phone = 'NULL'
        OR phone = '';

      -- Registro de reglas en field check
        INSERT INTO dqm_field_checks (table_name, column_name, rule_type, rule_param, description)
        VALUES 
        ('txt_shippers', 'shipper_id', 'TYPE', 'INTEGER', 'Debe ser entero'),
        ('txt_shippers', 'company_name', 'LENGTH', '100', 'Max 100 chars'),
        ('txt_shippers', 'phone', 'LENGTH', '50', 'Max 50 chars');

      -- Contador de filas validadas en la tabla actual
          EXECUTE 'SELECT COUNT(*) FROM txt_shippers' INTO v_current_table_rows;
          v_total_validated_rows := v_total_validated_rows + v_current_table_rows;



      -- TXT_SUPPLIERS TABLE -----------------------------------
        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_suppliers', supplier_id, 'supplier_id no es numérico'
        FROM txt_suppliers
        WHERE supplier_id !~ '^[0-9]+$'
        OR supplier_id = 'NULL'
        OR supplier_id = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_suppliers', supplier_id, 'company_name excede 100 caracteres'
        FROM txt_suppliers
        WHERE LENGTH(company_name) > 100
        OR company_name = 'NULL'
        OR company_name = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_suppliers', supplier_id, 'contact_name excede 100 caracteres'
        FROM txt_suppliers
        WHERE LENGTH(contact_name) > 100
        OR contact_name = 'NULL'
        OR contact_name = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_suppliers', supplier_id, 'contact_title excede 100 caracteres'
        FROM txt_suppliers
        WHERE LENGTH(contact_title) > 100
        OR contact_title = 'NULL'
        OR contact_title = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_suppliers', supplier_id, 'address excede 200 caracteres'
        FROM txt_suppliers
        WHERE LENGTH(address) > 200
        OR address = 'NULL'
        OR address = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_suppliers', supplier_id, 'city excede 50 caracteres'
        FROM txt_suppliers
        WHERE LENGTH(city) > 50
        OR city = 'NULL'
        OR city = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_suppliers', supplier_id, 'region excede 50 caracteres'
        FROM txt_suppliers
        WHERE LENGTH(region) > 50
        OR region = 'NULL'
        OR region = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_suppliers', supplier_id, 'postal_code excede 20 caracteres'
        FROM txt_suppliers
        WHERE LENGTH(postal_code) > 20
        OR postal_code = 'NULL'
        OR postal_code = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_suppliers', supplier_id, 'country excede 50 caracteres'
        FROM txt_suppliers
        WHERE LENGTH(country) > 50
        OR country = 'NULL'
        OR country = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_suppliers', supplier_id, 'phone excede 50 caracteres'
        FROM txt_suppliers
        WHERE LENGTH(phone) > 50
        OR phone = 'NULL'
        OR phone = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_suppliers', supplier_id, 'fax excede 50 caracteres'
        FROM txt_suppliers
        WHERE LENGTH(fax) > 50
        OR fax = 'NULL'
        OR fax = '';

      -- Registro de reglas en field check
        INSERT INTO dqm_field_checks (table_name, column_name, rule_type, rule_param, description)
        VALUES 
        ('txt_suppliers', 'supplier_id', 'TYPE', 'INTEGER', 'Debe ser entero'),
        ('txt_suppliers', 'company_name', 'LENGTH', '100', 'Max 100 chars'),
        ('txt_suppliers', 'contact_name', 'LENGTH', '100', 'Max 100 chars'),
        ('txt_suppliers', 'contact_title', 'LENGTH', '100', 'Max 100 chars'),
        ('txt_suppliers', 'address', 'LENGTH', '200', 'Max 200 chars'),
        ('txt_suppliers', 'city', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_suppliers', 'region', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_suppliers', 'postal_code', 'LENGTH', '20', 'Max 20 chars'),
        ('txt_suppliers', 'country', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_suppliers', 'phone', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_suppliers', 'fax', 'LENGTH', '50', 'Max 50 chars'),
        ('txt_suppliers', 'home_page', 'TYPE', 'TEXT', 'Debe ser texto');

      -- Contador de filas validadas en la tabla actual
          EXECUTE 'SELECT COUNT(*) FROM txt_suppliers' INTO v_current_table_rows;
          v_total_validated_rows := v_total_validated_rows + v_current_table_rows;




      -- TXT_PRODUCTS TABLE -----------------------------------
        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_products', product_id, 'product_id no es numérico'
        FROM txt_products
        WHERE product_id !~ '^[0-9]+$'
        OR product_id = 'NULL'
        OR product_id = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_products', product_id, 'product_name excede 100 caracteres'
        FROM txt_products
        WHERE LENGTH(product_name) > 100
        OR product_name = 'NULL'
        OR product_name = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_products', product_id, 'supplier_id no es numérico'
        FROM txt_products
        WHERE supplier_id !~ '^[0-9]+$'
        OR supplier_id = 'NULL'
        OR supplier_id = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_products', product_id, 'category_id no es numérico'
        FROM txt_products
        WHERE category_id !~ '^[0-9]+$'
        OR category_id = 'NULL'
        OR category_id = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_products', product_id, 'quantity_per_unit excede 100 caracteres'
        FROM txt_products
        WHERE LENGTH(quantity_per_unit) > 100
        OR quantity_per_unit = 'NULL'
        OR quantity_per_unit = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_products', product_id, 'unit_price formato inválido'
        FROM txt_products
        WHERE (unit_price !~ '^[0-9]+(\.[0-9]{1,2})?$'
        OR LENGTH(REPLACE(unit_price, '.', '')) > 10)
        OR unit_price = 'NULL'
        OR unit_price = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_products', product_id, 'units_in_stock no es entero'
        FROM txt_products
        WHERE units_in_stock !~ '^-?[0-9]+$'
        OR units_in_stock = 'NULL'
        OR units_in_stock = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_products', product_id, 'units_on_order no es entero'
        FROM txt_products
        WHERE units_on_order !~ '^-?[0-9]+$'
        OR units_on_order = 'NULL'
        OR units_on_order = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_products', product_id, 'reorder_level no es entero'
        FROM txt_products
        WHERE reorder_level !~ '^-?[0-9]+$'
        OR reorder_level = 'NULL'
        OR reorder_level = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_products', product_id, 'discontinued debe ser 0 o 1'
        FROM txt_products
        WHERE discontinued !~ '^(0|1)$'
        OR discontinued = 'NULL'
        OR discontinued = '';



      -- Registro de reglas en field check
        INSERT INTO dqm_field_checks (table_name, column_name, rule_type, rule_param,
            description)
        VALUES 
        ('txt_products', 'product_id', 'TYPE', 'INTEGER', 'Debe ser entero'),
        ('txt_products', 'product_name', 'LENGTH', '100', 'Max 100 chars'),
        ('txt_products', 'supplier_id', 'TYPE', 'INTEGER', 'Debe ser entero'),
        ('txt_products', 'category_id', 'TYPE', 'INTEGER', 'Debe ser entero'),
        ('txt_products', 'quantity_per_unit', 'LENGTH', '100', 'Max 100 chars'),
        ('txt_products', 'unit_price', 'FORMAT', '^-?[0-9]{1}(\\.[0-9]{1,2})?$', 'Formato invalido'),
        ('txt_products', 'units_in_stock', 'TYPE', 'INTEGER', 'Debe ser entero'),
        ('txt_products', 'units_on_order', 'TYPE', 'INTEGER', 'Debe ser entero'),
        ('txt_products', 'reorder_level', 'TYPE', 'INTEGER',  'Debe ser entero'),
        ('txt_products', 'discontinued', 'FORMAT',  '^(0|1)$',  'Debe ser 0 o 1');

      -- Contador de filas validadas en la tabla actual
          EXECUTE 'SELECT COUNT(*) FROM txt_products' INTO v_current_table_rows;
          v_total_validated_rows := v_total_validated_rows + v_current_table_rows;



      -- TXT_ORDERS TABLE -----------------------------------
      INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
      SELECT v_log_id, 'txt_orders', order_id, 'order_id no es numérico'
      FROM txt_orders
      WHERE order_id !~ '^[0-9]+$'
      OR order_id = 'NULL'
      OR order_id = '';

      INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
      SELECT v_log_id, 'txt_orders', order_id, 'customer_id excede 10 caracteres'
      FROM txt_orders
      WHERE LENGTH(customer_id) > 10
      OR customer_id = 'NULL'
      OR customer_id = '';

      INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
      SELECT v_log_id, 'txt_orders', order_id, 'employee_id no es numérico'
      FROM txt_orders
      WHERE employee_id !~ '^[0-9]+$'
      OR employee_id = 'NULL'
      OR employee_id = '';

      INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
      SELECT v_log_id, 'txt_orders', order_id, 'order_date formato inválido'
      FROM txt_orders
      WHERE (order_date IS NOT NULL
        AND order_date !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$')
        OR order_date = 'NULL'
        OR order_date = '';

      INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
      SELECT v_log_id, 'txt_orders', order_id, 'required_date formato inválido'
      FROM txt_orders
      WHERE (required_date IS NOT NULL
        AND required_date !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$')
        OR required_date = 'NULL'
        OR required_date = '';



        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_orders', order_id, 'shipped_date formato inválido'
        FROM txt_orders
        WHERE (shipped_date IS NOT NULL
        AND shipped_date !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$')
        OR shipped_date = 'NULL'
        OR shipped_date = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_orders', order_id, 'ship_via no es numérico'
        FROM txt_orders
        WHERE ship_via !~ '^[0-9]+$'
        OR ship_via = 'NULL'
        OR ship_via = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_orders', order_id, 'freight formato inválido'
        FROM txt_orders
        WHERE (freight !~ '^[0-9]+(\.[0-9]{1,2})?$'
        OR LENGTH(REPLACE(freight, '.', '')) > 10)
        OR freight = 'NULL'
        OR freight = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_orders', order_id, 'ship_name excede 100 caracteres'
        FROM txt_orders
        WHERE LENGTH(ship_name) > 100
        OR ship_name = 'NULL'
        OR ship_name = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_orders', order_id, 'ship_address excede 200 caracteres'
        FROM txt_orders
        WHERE LENGTH(ship_address) > 200
        OR ship_address = 'NULL'
        OR ship_address = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_orders', order_id, 'ship_city excede 50 caracteres'
        FROM txt_orders
        WHERE LENGTH(ship_city) > 50
        OR ship_city = 'NULL'
        OR ship_city = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_orders', order_id, 'ship_region excede 50 caracteres'
        FROM txt_orders
        WHERE LENGTH(ship_region) > 50
        OR ship_region = 'NULL'
        OR ship_region = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_orders', order_id, 'ship_postal_code excede 20 caracteres'
        FROM txt_orders
        WHERE LENGTH(ship_postal_code) > 20
        OR ship_postal_code = 'NULL'
        OR ship_postal_code = '';

        INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
        SELECT v_log_id, 'txt_orders', order_id, 'ship_country excede 50 caracteres'
        FROM txt_orders
        WHERE LENGTH(ship_country) > 50
        OR ship_country = 'NULL'
        OR ship_country = '';

   
      -- Registro de reglas en field check
      INSERT INTO dqm_field_checks (table_name, column_name, rule_type, rule_param,
        description)
      VALUES 
      ('txt_orders', 'order_id', 'TYPE', 'INTEGER', 'Debe ser entero'),
      ('txt_orders', 'customer_id', 'LENGTH', '10', 'Max 10 chars'),
      ('txt_orders', 'employee_id', 'TYPE', 'INTEGER', 'Debe ser entero'),
      ('txt_orders', 'order_date', 'FORMAT', 'YYYY-MM-DD HH24:MI:SS.MS', 'Formato de fecha invalido'),
      ('txt_orders', 'required_date', 'FORMAT', 'YYYY-MM-DD HH24:MI:SS.MS', 'Formato de fecha invalido'),
      ('txt_orders', 'shipped_date', 'FORMAT', 'YYYY-MM-DD HH24:MI:SS.MS', 'Formato de fecha invalido'),
      ('txt_orders', 'ship_via', 'TYPE', 'INTEGER', 'Debe ser entero'),
      ('txt_orders', 'freight', 'FORMAT', '^-?[0-9]{1}(\\.[0-9]{1,2})?$', 'Formato invalido'),
      ('txt_orders', 'ship_name', 'LENGTH',  '100', 'Max 100 chars'),
      ('txt_orders', 'ship_address', 'LENGTH',  '200',  'Max 200 chars'),
      ('txt_orders', 'ship_city', 'LENGTH',  '50',  'Max 50 chars'),
      ('txt_orders', 'ship_region', 'LENGTH',  '50',  'Max 50 chars'),
      ('txt_orders', 'ship_postal_code', 'LENGTH',  '20',  'Max 20 chars'),
      ('txt_orders', 'ship_country', 'LENGTH',  '50',  'Max 50 chars');

      -- Contador de filas validadas en la tabla actual
          EXECUTE 'SELECT COUNT(*) FROM txt_orders' INTO v_current_table_rows;
          v_total_validated_rows := v_total_validated_rows + v_current_table_rows;



      -- TXT_ORDER_DETAILS TABLE -----------------------------------
    INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
    SELECT v_log_id, 'txt_order_details', order_id, 'order_id no es numérico'
    FROM txt_order_details
    WHERE order_id !~ '^[0-9]+$'
    OR order_id = 'NULL'
    OR order_id = '';

    INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
    SELECT v_log_id, 'txt_order_details', order_id, 'product_id no es numérico'
    FROM txt_order_details
    WHERE product_id !~ '^[0-9]+$'
    OR product_id = 'NULL'
    OR product_id = '';

    INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
    SELECT v_log_id, 'txt_order_details', order_id, 'unit_price formato inválido'
    FROM txt_order_details
    WHERE (unit_price !~ '^[0-9]+(\.[0-9]{1,2})?$'
    OR LENGTH(REPLACE(unit_price, '.', '')) > 10)
    OR unit_price = 'NULL'
    OR unit_price = '';

    INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
    SELECT v_log_id, 'txt_order_details', order_id, 'quantity no es entero'
    FROM txt_order_details
    WHERE quantity !~ '^-?[0-9]+$'
    OR quantity = 'NULL'
    OR quantity = '';

    INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
    SELECT v_log_id, 'txt_order_details', order_id, 'discount formato inválido'
    FROM txt_order_details
    WHERE (discount !~ '^[0-9]+(\.[0-9]{1,2})?$'
    OR LENGTH(REPLACE(discount, '.', '')) > 10)
    OR discount = 'NULL'
    OR discount = '';
      -- Registro de reglas en field check
      INSERT INTO dqm_field_checks (table_name, column_name, rule_type, rule_param,
        description)
      VALUES 
      ('txt_order_details', 'order_id', 'TYPE', 'INTEGER', 'Debe ser entero'),
      ('txt_order_details', 'product_id', 'TYPE', 'INTEGER', 'Debe ser entero'),
      ('txt_order_details', 'unit_price', 'FORMAT', '^-?[0-9]{1}(\\.[0-9]{1,2})?$', 'Formato invalido'),
      ('txt_order_details', 'quantity', 'TYPE', 'INTEGER', 'Debe ser entero'),
      ('txt_order_details', 'discount', 'FORMAT',  '^-?[0-9]{1}(\\.[0-9]{1,2})?$',  'Formato invalido');

      -- Contador de filas validadas en la tabla actual
          EXECUTE 'SELECT COUNT(*) FROM txt_order_details' INTO v_current_table_rows;
          v_total_validated_rows := v_total_validated_rows + v_current_table_rows;




      -- TXT_REGION TABLE -----------------------------------

      INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
      SELECT v_log_id, 'txt_regions', region_id, 'region_id no es numérico'
      FROM txt_regions
      WHERE region_id !~ '^[0-9]+$'
      OR region_id = 'NULL'
      OR region_id = '';

      INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
      SELECT v_log_id, 'txt_regions', region_id, 'region_description excede 100 caracteres'
      FROM txt_regions
      WHERE LENGTH(region_description) > 100
      OR region_description = 'NULL'
      OR region_description = '';

      -- Registro de reglas en field check
      INSERT INTO dqm_field_checks (table_name, column_name, rule_type, rule_param,
        description)  
      VALUES 
      ('txt_regions', 'region_id', 'TYPE', 'INTEGER', 'Debe ser entero'),
      ('txt_regions', 'region_description', 'LENGTH', '100', 'Max 100 chars');

      -- Contador de filas validadas en la tabla actual
          EXECUTE 'SELECT COUNT(*) FROM txt_regions' INTO v_current_table_rows;
          v_total_validated_rows := v_total_validated_rows + v_current_table_rows;



      -- TXT_TERRITORIES TABLE -----------------------------------
      INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
      SELECT v_log_id, 'txt_territories', territory_id, 'territory_id excede 20 caracteres'
      FROM txt_territories
      WHERE LENGTH(territory_id) > 20
      OR territory_id = 'NULL'
      OR territory_id = '';

      INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
      SELECT v_log_id, 'txt_territories', territory_id, 'territory_description excede 100 caracteres'
      FROM txt_territories
      WHERE LENGTH(territory_description) > 100
      OR territory_description = 'NULL'
      OR territory_description = '';

      INSERT INTO dqm_validation_issues (log_id, table_name, row_key, issue_desc)
      SELECT v_log_id, 'txt_territories', territory_id, 'region_id no es numérico'
      FROM txt_territories
      WHERE region_id !~ '^[0-9]+$'
      OR region_id = 'NULL'
      OR region_id = '';

      -- Registro de reglas en field check
      INSERT INTO dqm_field_checks (table_name, column_name, rule_type, rule_param,
        description)  
      VALUES 
      ('txt_territories', 'territory_id', 'LENGTH', '20', 'Max 20 chars'),
      ('txt_territories', 'territory_description', 'LENGTH', '100', 'Max 100 chars'),
      ('txt_territories', 'region_id', 'TYPE', 'INTEGER', 'Debe ser entero');

      -- Contador de filas validadas en la tabla actual
          EXECUTE 'SELECT COUNT(*) FROM txt_territories' INTO v_current_table_rows;
          v_total_validated_rows := v_total_validated_rows + v_current_table_rows;

-------------------------------------------------------------------

-- Contamos cuántos errores encontramos en este log_id
        SELECT COUNT(*) INTO v_errors_count 
        FROM dqm_validation_issues 
        WHERE log_id = v_log_id;

        IF v_errors_count > 0 THEN
            -- Terminamos con advertencias
            UPDATE dqm_exec_log
            SET finished_at = NOW(),
                status = 'WARNING', -- O 'DATA_ERROR'
                message = 'Finalizado con ' || v_errors_count || ' inconsistencias detectadas.',
                rows_processed = v_errors_count -- Ojo: aqui decidi poner filas malas, puedes poner total
            WHERE log_id = v_log_id;
            
            RAISE NOTICE 'Script finalizado. Se encontraron % errores de datos. Ver tabla dqm_validation_issues log_id %', v_errors_count, v_log_id;
        ELSE
            -- Terminamos limpio
            UPDATE dqm_exec_log
            SET finished_at = NOW(),
                status = 'OK',
                message = 'Validación exitosa. Datos limpios.',
                rows_processed = 0
            WHERE log_id = v_log_id;
            
            RAISE NOTICE 'Script finalizado exitosamente sin errores.';
        END IF;

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




