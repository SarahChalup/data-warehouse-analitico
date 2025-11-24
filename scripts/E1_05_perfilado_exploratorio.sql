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
        'E1_05_perfilado_exploratorio.sql',
        'Perfilado de los datos y validación básica',
        'Agustina',
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


-- PERFILADO
-- 1. ANÁLISIS DE VOLUMEN Y COMPLETITUD


-- Conteo general de filas por tabla.

SELECT 'txt_categories' AS tabla, COUNT(*) AS total_filas FROM txt_categories UNION ALL
SELECT 'txt_customers' AS tabla, COUNT(*) AS total_filas FROM txt_customers UNION ALL
SELECT 'txt_employees' AS tabla, COUNT(*) AS total_filas FROM txt_employees UNION ALL
SELECT 'txt_order_details' AS tabla, COUNT(*) AS total_filas FROM txt_order_details UNION ALL
SELECT 'txt_orders' AS tabla, COUNT(*) AS total_filas FROM txt_orders UNION ALL
SELECT 'txt_products' AS tabla, COUNT(*) AS total_filas FROM txt_products UNION ALL
SELECT 'txt_shippers' AS tabla, COUNT(*) AS total_filas FROM txt_shippers UNION ALL
SELECT 'txt_suppliers' AS tabla, COUNT(*) AS total_filas FROM txt_suppliers;


-- Análisis de completitud para: : txt_orders

SELECT
    COUNT(*) AS total_ordenes,
    -- Contar filas donde shipped_date está vacío.
    COUNT(CASE WHEN shipped_date IS NULL OR shipped_date = '' THEN 1 END) AS ordenes_sin_enviar,
    -- Contar filas donde ship_region está vacío.
    COUNT(CASE WHEN ship_region IS NULL OR ship_region = '' THEN 1 END) AS ordenes_sin_region
FROM txt_orders;

-- Análisis de Completitud para: txt_suppliers


SELECT
    COUNT(*) AS total_proveedores,
    
    -- ¿Cuántos proveedores no tienen especificada una región?

    COUNT(CASE WHEN region IS NULL OR region = '' THEN 1 END) AS faltantes_region,
    
    -- ¿Cuántos no tienen un código postal?

    COUNT(CASE WHEN postal_code IS NULL OR postal_code = '' THEN 1 END) AS faltantes_codigo_postal,
    
    -- ¿Cuántos no tienen una página web registrada?

    COUNT(CASE WHEN home_page  IS NULL OR home_page = '' THEN 1 END) AS faltantes_homepage,
    
    -- ¿Cuántos no tienen un número de fax?

    COUNT(CASE WHEN fax IS NULL OR fax = '' THEN 1 END) AS faltantes_fax
    
FROM txt_suppliers;


-- Análisis de Completitud para: txt_customers

SELECT
    COUNT(*) AS total_clientes,
    
    -- ¿Cuántos clientes no tienen una región asignada?
    COUNT(CASE WHEN regions IS NULL OR regions = '' THEN 1 END) AS faltantes_region,
    
    -- ¿Cuántos códigos postales faltan?
    COUNT(CASE WHEN postal_code IS NULL OR postal_code = '' THEN 1 END) AS faltantes_codigo_postal,
    
    -- ¿Cuántos clientes no tienen un fax?
    COUNT(CASE WHEN fax IS NULL OR fax = '' THEN 1 END) AS faltantes_fax
    
FROM txt_customers;

-- Análisis de Completitud para: txt_employees

SELECT
    COUNT(*) AS total_empleados,
    
    -- ¿Cuántos empleados no tienen un jefe directo? 
    
    COUNT(CASE WHEN reports_to IS NULL OR reports_to = '' THEN 1 END) AS empleados_sin_jefe_directo,
    
    -- ¿A cuántos empleados les falta la región en su dirección?
    COUNT(CASE WHEN region IS NULL OR region = '' THEN 1 END) AS faltantes_region,
    
    -- ¿A cuántos les falta el teléfono de casa?
    COUNT(CASE WHEN home_phone IS NULL OR home_phone = '' THEN 1 END) AS faltantes_telefono_casa,
    
    -- ¿Cuántos no tienen una ruta para su foto?
    COUNT(CASE WHEN photo_path IS NULL OR photo_path = '' THEN 1 END) AS faltantes_ruta_foto

FROM txt_employees;


-- Análisis de Completitud para: txt_products

SELECT
    COUNT(*) AS total_productos,

    -- ¿Cuántos productos tienen el campo 'discontinued' vacío?
    COUNT(CASE WHEN discontinued IS NULL OR discontinued = '' THEN 1 END) AS faltantes_estado_discontinued,

    -- ¿Cuántos productos no tienen definido un nivel de reorden?
    COUNT(CASE WHEN reorder_level IS NULL OR reorder_level = '' THEN 1 END) AS faltantes_nivel_reorden,

    -- ¿Cuántos no tienen unidades en orden?

    COUNT(CASE WHEN units_on_order IS NULL OR units_on_order = '' THEN 1 END) AS faltantes_unidades_en_orden

FROM txt_products;


-- 2. ANÁLISIS DE CARDINALIDAD (VALORES ÚNICOS)


-- ¿Cuántos clientes, productos y proveedores distintos tenemos?

SELECT
    (SELECT COUNT(DISTINCT customer_id) FROM txt_customers) AS clientes_unicos,
    (SELECT COUNT(DISTINCT product_id) FROM txt_products) AS productos_unicos,
    (SELECT COUNT(DISTINCT supplier_id) FROM txt_suppliers) AS proveedores_unicos;

-- ¿Desde cuántos países diferentes provienen nuestros clientes y proveedores?

SELECT 'clientes' AS origen, COUNT(DISTINCT country) AS paises_distintos FROM txt_customers UNION ALL
SELECT 'proveedores' AS origen, COUNT(DISTINCT country) AS paises_distintos FROM txt_suppliers;

-- 3. ANÁLISIS DE RANGO Y DISTRIBUCIÓN


-- ¿Cuál es el rango de precios de nuestros productos? (mín, máx, promedio)

SELECT
    MIN(unit_price::numeric) AS precio_minimo,
    MAX(unit_price::numeric) AS precio_maximo,
    AVG(unit_price::numeric) AS precio_promedio
FROM txt_products
WHERE unit_price ~ '^[0-9]+(\.[0-9]+)?$';

-- ¿Cómo se distribuyen los productos por categoría?

SELECT c.category_name, COUNT(p.product_id) AS numero_de_productos
FROM txt_products p
JOIN txt_categories c ON p.category_id = c.category_id
GROUP BY c.category_name
ORDER BY numero_de_productos DESC;

-- ¿Cómo se distribuyen las órdenes por cliente? (TOP 10 clientes con más órdenes)

SELECT c.customer_name , COUNT(o.order_id) AS total_ordenes
FROM txt_orders o
JOIN txt_customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_name 
ORDER BY total_ordenes DESC
LIMIT 10;

-- 4. DETECCIÓN DE OUTLIERS (VALORES EXTREMOS)


-- ¿Cuáles son los 5 productos más caros y los 5 más baratos?
-- Más caros:
SELECT product_name, unit_price::numeric FROM txt_products
WHERE unit_price ~ '^[0-9]+(\.[0-9]+)?$' ORDER BY unit_price::numeric DESC LIMIT 5;
-- Más baratos:
SELECT product_name, unit_price::numeric FROM txt_products
WHERE unit_price ~ '^[0-9]+(\.[0-9]+)?$' ORDER BY unit_price::numeric ASC LIMIT 5;


-- ¿Cuáles son las 5 órdenes con el costo de envío (freight) más alto?
SELECT order_id, customer_id, ship_country, freight::numeric
FROM txt_orders
WHERE freight ~ '^[0-9]+(\.[0-9]+)?$'
ORDER BY freight::numeric DESC
LIMIT 5;

-- ¿Cuáles son los 5 productos con más y menos stock?
-- Más stock:
SELECT product_name, units_in_stock::numeric FROM txt_products
WHERE units_in_stock ~ '^[0-9]+$' ORDER BY units_in_stock::numeric DESC LIMIT 5;
-- Menos stock (sin contar los que tienen 0):
SELECT product_name, units_in_stock::numeric FROM txt_products
WHERE units_in_stock ~ '^[0-9]+$' AND units_in_stock::numeric > 0 ORDER BY units_in_stock::numeric ASC LIMIT 5;


