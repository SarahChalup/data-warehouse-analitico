DO $$
DECLARE
    v_log_id BIGINT;
    v_script_id INT := 1;  -- ejemplo
BEGIN
	
	INSERT INTO dqm_scripts_inventory (
	    script_name,
	    description,
	    filename,
	    created_by,
		created_at
	)
	VALUES (
	    'crear_tablas_txt',
	    'Crea las tablas TXT_* para la etapa de Adquisición',
	    '01_3a_crear_tablas_txt.sql',
	    'Sarah',
		NOW()
	)
	RETURNING script_id INTO v_script_id;


    INSERT INTO dqm_exec_log (script_id, started_at, status)
    VALUES (v_script_id, NOW(), 'RUNNING')
    RETURNING log_id INTO v_log_id;

-----------------------------------------------------------
-- Inicio del proceso


        CREATE TABLE txt_categories
        (      
            category_id TEXT,
            category_name TEXT,
            description TEXT,
            picture TEXT
        );


        CREATE TABLE txt_customers
        (      
            customer_id TEXT,
            customer_name TEXT,
            contact_name TEXT,
            contact_title TEXT,
            address TEXT,
            city TEXT,
            regions TEXT,
            postal_code TEXT,
            country TEXT,
            phone TEXT,
            fax TEXT
        );

        CREATE TABLE txt_employee_territories
        (
            employee_id TEXT,
            territory_id TEXT

        );


        CREATE TABLE txt_employees
        (
            employee_id TEXT,
            last_name TEXT,
            first_name TEXT,
            title TEXT,
            title_of_courtesy TEXT,
            birth_date TEXT,
            hire_date TEXT,
            address TEXT,
            city TEXT,
            region TEXT,
            postal_code TEXT,
            country TEXT,
            home_phone TEXT,
            extension TEXT,
            photo TEXT,
            notes TEXT,
            reports_to TEXT,
            photo_path TEXT
        );


        CREATE TABLE txt_shippers(
            shipper_id TEXT,
            company_name TEXT,
            phone TEXT
        );


        CREATE TABLE txt_suppliers(
            supplier_id TEXT,
            company_name TEXT,
            contact_name TEXT,
            contact_title TEXT,
            address TEXT,
            city TEXT,
            region TEXT,
            postal_code TEXT,
            country TEXT,
            phone TEXT,
            fax TEXT,
            home_page TEXT
        );


        CREATE TABLE txt_products(
            product_id TEXT,
            product_name TEXT,
            supplier_id TEXT,
            category_id TEXT,
            quantity_per_unit TEXT,
            unit_price TEXT,
            units_in_stock TEXT,
            units_on_order TEXT,
            reorder_level TEXT,
            discontinued TEXT
        );


        CREATE TABLE txt_orders(
            order_id TEXT,
            customer_id TEXT,
            employee_id TEXT,
            order_date TEXT,
            required_date TEXT,
            shipped_date TEXT,
            ship_via TEXT,
            freight TEXT,
            ship_name TEXT,
            ship_address TEXT,
            ship_city TEXT,
            ship_region TEXT,
            ship_postal_code TEXT,
            ship_country TEXT
        );


        CREATE TABLE txt_order_details(    
            order_id TEXT,
            product_id TEXT,
            unit_price TEXT,
            quantity TEXT,
            discount TEXT
        );

        CREATE TABLE txt_regions
        (
            region_id TEXT,
            region_description TEXT
        );

        CREATE TABLE txt_territories
        (
            territory_id TEXT,
            territory_description TEXT,
            region_id TEXT
        );


-- Fin del proceso
-------------------------------------------------------------------


    UPDATE dqm_exec_log
    SET finished_at = NOW(),
        status = 'OK',
        message = 'Completado',
		rows_processed = 0
    WHERE log_id = v_log_id;
END $$;