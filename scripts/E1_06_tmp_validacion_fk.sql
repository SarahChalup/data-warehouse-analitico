---------------------------------------------------------------
--  E1_06_tmp_validacion_fk.sql
--  Validación de integridad referencial (FK) sobre tablas TMP
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
        'E1_06_tmp_validacion_fk.sql',
        'Valida integridad referencial (FK) entre tablas TMP',
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
    -- 3. Cerrar el log del proceso de registro
    -------------------------------------------------------------------
    UPDATE dqm_exec_log
    SET finished_at   = NOW(),
        status        = 'OK',
        message       = 'Script de validación de FK ejecutado: revisar SELECT posteriores',
        rows_processed = 0
    WHERE log_id = v_log_id;

END $$;

---------------------------------------------------------------
-- VALIDACIONES DE FK ENTRE TABLAS TMP
-- Cada SELECT busca registros "huérfanos" en la tabla hija
-- (si devuelve 0 filas, la FK está OK)
---------------------------------------------------------------


---------------------------------------------------------------
-- 1) PRODUCTS.supplier_id -> SUPPLIERS.supplier_id
---------------------------------------------------------------
SELECT p.*
FROM tmp_products p
LEFT JOIN tmp_suppliers s
       ON p.supplier_id = s.supplier_id
WHERE p.supplier_id IS NOT NULL
  AND s.supplier_id IS NULL;


---------------------------------------------------------------
-- 2) PRODUCTS.category_id -> CATEGORIES.category_id
---------------------------------------------------------------
SELECT p.*
FROM tmp_products p
LEFT JOIN tmp_categories c
       ON p.category_id = c.category_id
WHERE p.category_id IS NOT NULL
  AND c.category_id IS NULL;


---------------------------------------------------------------
-- 3) ORDERS.customer_id -> CUSTOMERS.customer_id
---------------------------------------------------------------
SELECT o.*
FROM tmp_orders o
LEFT JOIN tmp_customers c
       ON o.customer_id = c.customer_id
WHERE o.customer_id IS NOT NULL
  AND c.customer_id IS NULL;


---------------------------------------------------------------
-- 4) ORDERS.employee_id -> EMPLOYEES.employee_id
---------------------------------------------------------------
SELECT o.*
FROM tmp_orders o
LEFT JOIN tmp_employees e
       ON o.employee_id = e.employee_id
WHERE o.employee_id IS NOT NULL
  AND e.employee_id IS NULL;


---------------------------------------------------------------
-- 5) ORDERS.ship_via -> SHIPPERS.shipper_id
---------------------------------------------------------------
SELECT o.*
FROM tmp_orders o
LEFT JOIN tmp_shippers s
       ON o.ship_via = s.shipper_id
WHERE o.ship_via IS NOT NULL
  AND s.shipper_id IS NULL;


---------------------------------------------------------------
-- 6) ORDER_DETAILS.order_id -> ORDERS.order_id
---------------------------------------------------------------
SELECT od.*
FROM tmp_order_details od
LEFT JOIN tmp_orders o
       ON od.order_id = o.order_id
WHERE od.order_id IS NOT NULL
  AND o.order_id IS NULL;


---------------------------------------------------------------
-- 7) ORDER_DETAILS.product_id -> PRODUCTS.product_id
---------------------------------------------------------------
SELECT od.*
FROM tmp_order_details od
LEFT JOIN tmp_products p
       ON od.product_id = p.product_id
WHERE od.product_id IS NOT NULL
  AND p.product_id IS NULL;


---------------------------------------------------------------
-- 8) EMPLOYEE_TERRITORIES.employee_id -> EMPLOYEES.employee_id
---------------------------------------------------------------
SELECT et.*
FROM tmp_employee_territories et
LEFT JOIN tmp_employees e
       ON et.employee_id = e.employee_id
WHERE et.employee_id IS NOT NULL
  AND e.employee_id IS NULL;


---------------------------------------------------------------
-- 9) EMPLOYEE_TERRITORIES.territory_id -> TERRITORIES.territory_id
---------------------------------------------------------------
SELECT et.*
FROM tmp_employee_territories et
LEFT JOIN tmp_territories t
       ON et.territory_id = t.territory_id
WHERE et.territory_id IS NOT NULL
  AND t.territory_id IS NULL;


---------------------------------------------------------------
-- 10) TERRITORIES.region_id -> REGION.region_id
---------------------------------------------------------------
SELECT t.*
FROM tmp_territories t
LEFT JOIN tmp_region r
       ON t.region_id = r.region_id
WHERE t.region_id IS NOT NULL
  AND r.region_id IS NULL;


---------------------------------------------------------------
-- 11) CUSTOMER_CUSTOMER_DEMO.customer_id -> CUSTOMERS.customer_id
---------------------------------------------------------------
SELECT cd.*
FROM tmp_customer_customer_demo cd
LEFT JOIN tmp_customers c
       ON cd.customer_id = c.customer_id
WHERE cd.customer_id IS NOT NULL
  AND c.customer_id IS NULL;


---------------------------------------------------------------
-- 12) CUSTOMER_CUSTOMER_DEMO.customer_type_id 
--     -> CUSTOMER_DEMOGRAPHICS.customer_type_id
---------------------------------------------------------------
SELECT cd.*
FROM tmp_customer_customer_demo cd
LEFT JOIN tmp_customer_demographics d
       ON cd.customer_type_id = d.customer_type_id
WHERE cd.customer_type_id IS NOT NULL
  AND d.customer_type_id IS NULL;


---------------------------------------------------------------
-- 13) EMPLOYEES.reports_to -> EMPLOYEES.employee_id (autorreferencia)
---------------------------------------------------------------
SELECT e.*
FROM tmp_employees e
LEFT JOIN tmp_employees jefe
       ON e.reports_to = jefe.employee_id
WHERE e.reports_to IS NOT NULL
  AND jefe.employee_id IS NULL;


