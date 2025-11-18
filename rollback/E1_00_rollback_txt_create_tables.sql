-- ============================================
-- 1. Registrar este script en el inventario
-- ============================================
WITH new_script AS (
    INSERT INTO dqm_scripts_inventory (
        script_name, description, created_by, created_at
    )
    VALUES (
        'E1_00_rollback_txt_create_tables.sql',
        'Rollback de tablas TXT creadadas en etapa de Adquisición',
        'Sarah',
        NOW()
    )
    RETURNING script_id
),

-- ============================================
-- 2. Registrar inicio del log
-- ============================================
new_log AS (
    INSERT INTO dqm_exec_log (script_id, started_at, status)
    SELECT script_id, NOW(), 'RUNNING'
    FROM new_script
    RETURNING log_id, script_id
),

-- ============================================
-- 3. Eliminar tablas en orden seguro
-- ============================================
drop_tables AS (
    -- DROP 100% SQL. No loops permitidos aquí.
    SELECT 
        (DROP TABLE IF EXISTS txt_order_details        CASCADE) AS d1,
        (DROP TABLE IF EXISTS txt_orders               CASCADE) AS d2,
        (DROP TABLE IF EXISTS txt_products             CASCADE) AS d3,
        (DROP TABLE IF EXISTS txt_suppliers            CASCADE) AS d4,
        (DROP TABLE IF EXISTS txt_shippers             CASCADE) AS d5,
        (DROP TABLE IF EXISTS txt_employees            CASCADE) AS d6,
        (DROP TABLE IF EXISTS txt_employee_territories CASCADE) AS d7,
        (DROP TABLE IF EXISTS txt_territories          CASCADE) AS d8,
        (DROP TABLE IF EXISTS txt_regions              CASCADE) AS d9,
        (DROP TABLE IF EXISTS txt_customers            CASCADE) AS d10,
        (DROP TABLE IF EXISTS txt_categories           CASCADE) AS d11
)

-- ============================================
-- 4. Registrar eliminación de objetos
-- ============================================
INSERT INTO dqm_object_inventory (object_name, object_type, created_by_script, created_at)
SELECT 
    tbl AS object_name,
    'table_drop' AS object_type,
    script_id,
    NOW()
FROM (
    VALUES 
        ('txt_order_details'),
        ('txt_orders'),
        ('txt_products'),
        ('txt_suppliers'),
        ('txt_shippers'),
        ('txt_employees'),
        ('txt_employee_territories'),
        ('txt_territories'),
        ('txt_regions'),
        ('txt_customers'),
        ('txt_categories')
) AS t(tbl)
CROSS JOIN new_log;


-- ============================================
-- 5. Cerrar log
-- ============================================
UPDATE dqm_exec_log
SET finished_at = NOW(),
    status = 'OK',
    message = 'Rollback de tablas TXT completado',
    rows_processed = 0
WHERE log_id = (SELECT log_id FROM new_log);