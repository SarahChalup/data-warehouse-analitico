---------------------------------------------------------------
--  E1_04_validacion_pk.sql
--  Validación de claves primarias para tablas TXT
---------------------------------------------------------------

DO $$
DECLARE
    v_log_id    BIGINT;
    v_script_id INT;
BEGIN
    -------------------------------------------------------------------
    -- 1. Registrar el script en dqm_scripts_inventory
    -------------------------------------------------------------------
    INSERT INTO dqm_scripts_inventory (
        script_name,
        description,
        created_by,
        created_at
    )
    VALUES (
        'E3_05_validacion_pk.sql',
        'Validación de formato, nulos y duplicados de PK en tablas TXT',
        'Mariana',
        NOW()
    )
    RETURNING script_id INTO v_script_id;

    -------------------------------------------------------------------
    -- 2. Registrar inicio de ejecución en dqm_exec_log
    -------------------------------------------------------------------
    INSERT INTO dqm_exec_log (script_id, started_at, status)
    VALUES (v_script_id, NOW(), 'RUNNING')
    RETURNING log_id INTO v_log_id;

    -------------------------------------------------------------------
    -- 3. Cerrar el log del proceso (los SELECT se ejecutan fuera del DO)
    -------------------------------------------------------------------
    UPDATE dqm_exec_log
    SET finished_at   = NOW(),
        status        = 'OK',
        message       = 'Script de validación ejecutado: revisar SELECT posteriores',
        rows_processed = 0
    WHERE log_id = v_log_id;

END $$;

---------------------------------------------------------------
-- VALIDACIONES PK POR TABLA TXT 
---------------------------------------------------------------


---------------------------------------------------------------
-- 1. txt_categories  (PK TMP: category_id INTEGER)
---------------------------------------------------------------
SELECT COUNT(*) AS null_or_empty
FROM txt_categories
WHERE category_id IS NULL OR TRIM(category_id) = '';

SELECT category_id
FROM txt_categories
WHERE category_id !~ '^[0-9]+$';

SELECT category_id, COUNT(*) AS veces
FROM txt_categories
GROUP BY category_id
HAVING COUNT(*) > 1;


---------------------------------------------------------------
-- 2. txt_customers  (PK TMP: customer_id VARCHAR(10))
---------------------------------------------------------------
SELECT COUNT(*) AS null_or_empty
FROM txt_customers
WHERE customer_id IS NULL OR TRIM(customer_id) = '';

SELECT customer_id, LENGTH(customer_id) AS len
FROM txt_customers
WHERE LENGTH(customer_id) > 10;

SELECT customer_id, COUNT(*) AS veces
FROM txt_customers
group by customer_id
HAVING COUNT(*) > 1;


---------------------------------------------------------------
-- 3. txt_employee_territories (PK TMP compuesta)
---------------------------------------------------------------
SELECT COUNT(*) AS nulls_in_pk
FROM txt_employee_territories
WHERE employee_id IS NULL OR TRIM(employee_id) = ''
   OR territory_id IS NULL OR TRIM(territory_id) = '';

SELECT employee_id
FROM txt_employee_territories
WHERE employee_id !~ '^[0-9]+$';

SELECT territory_id, LENGTH(territory_id) AS len
FROM txt_employee_territories
WHERE LENGTH(territory_id) > 20;

SELECT employee_id, territory_id, COUNT(*) AS veces
FROM txt_employee_territories
GROUP BY employee_id, territory_id
HAVING COUNT(*) > 1;


---------------------------------------------------------------
-- 4. txt_employees (PK TMP: employee_id INTEGER)
---------------------------------------------------------------
SELECT COUNT(*) AS null_or_empty
FROM txt_employees
WHERE employee_id IS NULL OR TRIM(employee_id) = '';

SELECT employee_id
FROM txt_employees
WHERE employee_id !~ '^[0-9]+$';

SELECT employee_id, COUNT(*) AS veces
FROM txt_employees
GROUP BY employee_id
HAVING COUNT(*) > 1;


---------------------------------------------------------------
-- 5. txt_shippers (PK TMP: shipper_id INTEGER)
---------------------------------------------------------------
SELECT COUNT(*) AS null_or_empty
FROM txt_shippers
WHERE shipper_id IS NULL OR TRIM(shipper_id) = '';

SELECT shipper_id
FROM txt_shippers
WHERE shipper_id !~ '^[0-9]+$';

SELECT shipper_id, COUNT(*) AS veces
FROM txt_shippers
GROUP BY shipper_id
HAVING COUNT(*) > 1;


---------------------------------------------------------------
-- 6. txt_suppliers (PK TMP: supplier_id INTEGER)
---------------------------------------------------------------
SELECT COUNT(*) AS null_or_empty
FROM txt_suppliers
WHERE supplier_id IS NULL OR TRIM(supplier_id) = '';

SELECT supplier_id
FROM txt_suppliers
WHERE supplier_id !~ '^[0-9]+$';

SELECT supplier_id, COUNT(*) AS veces
FROM txt_suppliers
group by supplier_id
HAVING COUNT(*) > 1;


---------------------------------------------------------------
-- 7. txt_products (PK TMP: product_id INTEGER)
---------------------------------------------------------------
SELECT COUNT(*) AS null_or_empty
FROM txt_products
WHERE product_id IS NULL OR TRIM(product_id) = '';

SELECT product_id
FROM txt_products
WHERE product_id !~ '^[0-9]+$';

SELECT product_id, COUNT(*) AS veces
FROM txt_products
GROUP BY product_id
HAVING COUNT(*) > 1;


---------------------------------------------------------------
-- 8. txt_orders (PK TMP: order_id INTEGER)
---------------------------------------------------------------
SELECT COUNT(*) AS null_or_empty
FROM txt_orders
WHERE order_id IS NULL OR TRIM(order_id) = '';

SELECT order_id
FROM txt_orders
WHERE order_id !~ '^[0-9]+$';

SELECT order_id, COUNT(*) AS veces
FROM txt_orders
GROUP BY order_id
HAVING COUNT(*) > 1;


---------------------------------------------------------------
-- 9. txt_order_details (PK TMP compuesta: order_id, product_id)
---------------------------------------------------------------
SELECT COUNT(*) AS nulls_in_pk
FROM txt_order_details
WHERE order_id IS NULL OR TRIM(order_id) = ''
   OR product_id IS NULL OR TRIM(product_id) = '';

SELECT order_id, product_id
FROM txt_order_details
WHERE order_id !~ '^[0-9]+$'
   OR product_id !~ '^[0-9]+$';

SELECT order_id, product_id, COUNT(*) AS veces
FROM txt_order_details
GROUP BY order_id, product_id
HAVING COUNT(*) > 1;


---------------------------------------------------------------
-- 10. txt_regions (PK TMP: region_id INTEGER)
---------------------------------------------------------------
SELECT COUNT(*) AS null_or_empty
FROM txt_regions
WHERE region_id IS NULL OR TRIM(region_id) = '';

SELECT region_id
FROM txt_regions
WHERE region_id !~ '^[0-9]+$';

SELECT region_id, COUNT(*) AS veces
FROM txt_regions
group by region_id
HAVING COUNT(*) > 1;


---------------------------------------------------------------
-- 11. txt_territories (PK TMP: territory_id VARCHAR(20))
---------------------------------------------------------------
SELECT COUNT(*) AS null_or_empty
FROM txt_territories
WHERE territory_id IS NULL OR TRIM(territory_id) = '';

SELECT territory_id, LENGTH(territory_id) AS len
FROM txt_territories
WHERE LENGTH(territory_id) > 20;

SELECT territory_id, COUNT(*) AS veces
FROM txt_territories
group by territory_id
HAVING COUNT(*) > 1;


---------------------------------------------------------------
-- 12. txt_country (PK TMP: territory_id VARCHAR(20))
---------------------------------------------------------------
SELECT COUNT(*) AS null_or_empty
FROM txt_countries
WHERE country_name IS NULL OR TRIM(country_name) = '';


SELECT country_name, COUNT(*) AS veces
FROM txt_countries
group by country_name
HAVING COUNT(*) > 1;