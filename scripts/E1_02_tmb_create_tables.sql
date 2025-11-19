DO $$
DECLARE
    v_log_id BIGINT;
    v_script_id INT := 1;  -- ejemplo
BEGIN
-- 1. Registro del script en el inventario
	INSERT INTO dqm_scripts_inventory (
	    script_name, description, created_by, created_at  
	)
	VALUES (
	    'E1_02_tmb_create_tables.sql',
	    'Crea las tablas TMB para la etapa de Adquisición',
	    'Mariana',
		NOW()
	)
	RETURNING script_id INTO v_script_id;

-- 2. Registro del inicio de ejecución en el log
    INSERT INTO dqm_exec_log (script_id, started_at, status)
    VALUES (v_script_id, NOW(), 'RUNNING')
    RETURNING log_id INTO v_log_id;


    -------------------------------------------------------------------
    -- 3. INICIO DEL PROCESO: CREACIÓN DE TABLAS TMP
    -------------------------------------------------------------------

 -- 1. CATEGORIES
    CREATE TABLE tmp_categories (
        category_id      INTEGER PRIMARY KEY,
        category_name    VARCHAR(50),
        description      TEXT,
        picture          BYTEA
    );

    INSERT INTO dqm_object_inventory (
        object_name, object_type, created_by_script, created_at
    ) VALUES (
        'tmp_categories', 'table', v_script_id, NOW()
    );

    -------------------------------------------------------------------
    -- 2. SUPPLIERS
    -------------------------------------------------------------------
    CREATE TABLE tmp_suppliers (
        supplier_id     INTEGER PRIMARY KEY,
        company_name    VARCHAR(100),
        contact_name    VARCHAR(100),
        contact_title   VARCHAR(100),
        address         VARCHAR(200),
        city            VARCHAR(50),
        region          VARCHAR(50),
        postal_code     VARCHAR(20),
        country         VARCHAR(50),
        phone           VARCHAR(50),
        fax             VARCHAR(50),
        homepage        TEXT
    );

    INSERT INTO dqm_object_inventory (
        object_name, object_type, created_by_script, created_at
    ) VALUES (
        'tmp_suppliers', 'table', v_script_id, NOW()
    );

    -------------------------------------------------------------------
    -- 3. PRODUCTS
    -------------------------------------------------------------------
    CREATE TABLE tmp_products (
        product_id        INTEGER PRIMARY KEY,
        product_name      VARCHAR(100),
        supplier_id       INTEGER,
        category_id       INTEGER,
        quantity_per_unit VARCHAR(100),
        unit_price        NUMERIC(10,2),
        units_in_stock    INTEGER,
        units_on_order    INTEGER,
        reorder_level     INTEGER,
        discontinued      INTEGER
        -- FOREIGN KEY (supplier_id) REFERENCES tmp_suppliers(supplier_id),
        -- FOREIGN KEY (category_id) REFERENCES tmp_categories(category_id)
    );

    INSERT INTO dqm_object_inventory (
        object_name, object_type, created_by_script, created_at
    ) VALUES (
        'tmp_products', 'table', v_script_id, NOW()
    );

    -------------------------------------------------------------------
    -- 4. CUSTOMERS
    -------------------------------------------------------------------
    CREATE TABLE tmp_customers (
        customer_id    VARCHAR(10) PRIMARY KEY,
        company_name   VARCHAR(100),
        contact_name   VARCHAR(100),
        contact_title  VARCHAR(100),
        address        VARCHAR(200),
        city           VARCHAR(50),
        region         VARCHAR(50),
        postal_code    VARCHAR(20),
        country        VARCHAR(50),
        phone          VARCHAR(50),
        fax            VARCHAR(50)
    );

    INSERT INTO dqm_object_inventory (
        object_name, object_type, created_by_script, created_at
    ) VALUES (
        'tmp_customers', 'table', v_script_id, NOW()
    );

    -------------------------------------------------------------------
    -- 5. CUSTOMER DEMOGRAPHICS
    -------------------------------------------------------------------
    CREATE TABLE tmp_customer_demographics (
        customer_type_id VARCHAR(10) PRIMARY KEY,
        customer_desc    TEXT
    );

    INSERT INTO dqm_object_inventory (
        object_name, object_type, created_by_script, created_at
    ) VALUES (
        'tmp_customer_demographics', 'table', v_script_id, NOW()
    );

    -------------------------------------------------------------------
    -- 6. CUSTOMER – CUSTOMER DEMO (relación N a N)
    -------------------------------------------------------------------
    CREATE TABLE tmp_customer_customer_demo (
        customer_id      VARCHAR(10),
        customer_type_id VARCHAR(10),
        PRIMARY KEY (customer_id, customer_type_id)
        -- FOREIGN KEY (customer_id) REFERENCES tmp_customers(customer_id),
        -- FOREIGN KEY (customer_type_id) REFERENCES tmp_customer_demographics(customer_type_id)
    );

    INSERT INTO dqm_object_inventory (
        object_name, object_type, created_by_script, created_at
    ) VALUES (
        'tmp_customer_customer_demo', 'table', v_script_id, NOW()
    );

    -------------------------------------------------------------------
    -- 7. EMPLOYEES
    -------------------------------------------------------------------
    CREATE TABLE tmp_employees (
        employee_id       INTEGER PRIMARY KEY,
        last_name         VARCHAR(50),
        first_name        VARCHAR(50),
        title             VARCHAR(100),
        title_of_courtesy VARCHAR(50),
        birth_date        DATE,
        hire_date         DATE,
        address           VARCHAR(200),
        city              VARCHAR(50),
        region            VARCHAR(50),
        postal_code       VARCHAR(20),
        country           VARCHAR(50),
        home_phone        VARCHAR(50),
        extension         VARCHAR(10),
        photo             BYTEA,
        notes             TEXT,
        reports_to        INTEGER,
        photo_path        VARCHAR(200)
        -- FOREIGN KEY (reports_to) REFERENCES tmp_employees(employee_id)
    );

    INSERT INTO dqm_object_inventory (
        object_name, object_type, created_by_script, created_at
    ) VALUES (
        'tmp_employees', 'table', v_script_id, NOW()
    );

    -------------------------------------------------------------------
    -- 8. ORDERS
    -------------------------------------------------------------------
    CREATE TABLE tmp_orders (
        order_id         INTEGER PRIMARY KEY,
        customer_id      VARCHAR(10),
        employee_id      INTEGER,
        order_date       DATE,
        required_date    DATE,
        shipped_date     DATE,
        ship_via         INTEGER,
        freight          NUMERIC(10,2),
        ship_name        VARCHAR(100),
        ship_address     VARCHAR(200),
        ship_city        VARCHAR(50),
        ship_region      VARCHAR(50),
        ship_postal_code VARCHAR(20),
        ship_country     VARCHAR(50)
        -- FOREIGN KEY (customer_id) REFERENCES tmp_customers(customer_id),
        -- FOREIGN KEY (employee_id) REFERENCES tmp_employees(employee_id),
        -- FOREIGN KEY (ship_via) REFERENCES tmp_shippers(shipper_id)
    );

    INSERT INTO dqm_object_inventory (
        object_name, object_type, created_by_script, created_at
    ) VALUES (
        'tmp_orders', 'table', v_script_id, NOW()
    );

    -------------------------------------------------------------------
    -- 9. ORDER DETAILS
    -------------------------------------------------------------------
    CREATE TABLE tmp_order_details (
        order_id   INTEGER,
        product_id INTEGER,
        unit_price NUMERIC(10,2),
        quantity   INTEGER,
        discount   NUMERIC(4,2),
        PRIMARY KEY (order_id, product_id)
        -- FOREIGN KEY (order_id) REFERENCES tmp_orders(order_id),
        -- FOREIGN KEY (product_id) REFERENCES tmp_products(product_id)
    );

    INSERT INTO dqm_object_inventory (
        object_name, object_type, created_by_script, created_at
    ) VALUES (
        'tmp_order_details', 'table', v_script_id, NOW()
    );

    -------------------------------------------------------------------
    -- 10. SHIPPERS
    -------------------------------------------------------------------
    CREATE TABLE tmp_shippers (
        shipper_id   INTEGER PRIMARY KEY,
        company_name VARCHAR(100),
        phone        VARCHAR(50)
    );

    INSERT INTO dqm_object_inventory (
        object_name, object_type, created_by_script, created_at
    ) VALUES (
        'tmp_shippers', 'table', v_script_id, NOW()
    );

    -------------------------------------------------------------------
    -- 11. REGION
    -------------------------------------------------------------------
    CREATE TABLE tmp_region (
        region_id          INTEGER PRIMARY KEY,
        region_description VARCHAR(100)
    );

    INSERT INTO dqm_object_inventory (
        object_name, object_type, created_by_script, created_at
    ) VALUES (
        'tmp_region', 'table', v_script_id, NOW()
    );

    -------------------------------------------------------------------
    -- 12. TERRITORIES
    -------------------------------------------------------------------
    CREATE TABLE tmp_territories (
        territory_id         VARCHAR(20) PRIMARY KEY,
        territory_description VARCHAR(100),
        region_id            INTEGER
        -- FOREIGN KEY (region_id) REFERENCES tmp_region(region_id)
    );

    INSERT INTO dqm_object_inventory (
        object_name, object_type, created_by_script, created_at
    ) VALUES (
        'tmp_territories', 'table', v_script_id, NOW()
    );

    -------------------------------------------------------------------
    -- 13. EMPLOYEE_TERRITORIES
    -------------------------------------------------------------------
    CREATE TABLE tmp_employee_territories (
        employee_id  INTEGER,
        territory_id VARCHAR(20),
        PRIMARY KEY (employee_id, territory_id)
        -- FOREIGN KEY (employee_id) REFERENCES tmp_employees(employee_id),
        -- FOREIGN KEY (territory_id) REFERENCES tmp_territories(territory_id)
    );

    INSERT INTO dqm_object_inventory (
        object_name, object_type, created_by_script, created_at
    ) VALUES (
        'tmp_employee_territories', 'table', v_script_id, NOW()
    );

    -------------------------------------------------------------------
    -- 14. US STATES
    -------------------------------------------------------------------
    CREATE TABLE tmp_us_states (
        state_id     INTEGER PRIMARY KEY,
        state_name   VARCHAR(50),
        state_abbr   VARCHAR(5),
        state_region VARCHAR(50)
    );

    INSERT INTO dqm_object_inventory (
        object_name, object_type, created_by_script, created_at
    ) VALUES (
        'tmp_us_states', 'table', v_script_id, NOW()
    );

    -------------------------------------------------------------------
    -- 4. FIN DEL PROCESO: ACTUALIZAR LOG DE EJECUCIÓN
    -------------------------------------------------------------------
    UPDATE dqm_exec_log
    SET finished_at   = NOW(),
        status        = 'OK',
        message       = 'Completado',
        rows_processed = 0
    WHERE log_id = v_log_id;

END $$;