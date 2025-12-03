---------------------------------------------------------------
-- E3_11_metadata_update_actualizacion.sql
-- Actualiza la Metadata de la Etapa 3 – Actualización
-- - Documenta capa de Memoria (tablas DWM, columna ACTIVE)
-- - Documenta nuevas columnas country_key en dimensiones
-- - Documenta relaciones con dim_country.country_key
-- Autora: Mariana
---------------------------------------------------------------

DO $$
DECLARE
    v_log_id        BIGINT;
    v_script_id     INT;
    v_entity_id     INT;
    v_script_name   TEXT := 'E3_11_metadata_update_actualizacion.sql';
    v_script_desc   TEXT := 'Actualiza Metadata para Etapa 3: país + memoria (DWM) + nuevas columnas en dimensiones.';
    v_user          TEXT := 'Mariana';
BEGIN
    -------------------------------------------------------------------
    -- 1. Registrar este script en dqm_scripts_inventory (si no existe)
    -------------------------------------------------------------------
    SELECT script_id
    INTO   v_script_id
    FROM   dqm_scripts_inventory
    WHERE  script_name = v_script_name;

    IF v_script_id IS NULL THEN
        INSERT INTO dqm_scripts_inventory (
            script_name,
            description,
            created_by,
            created_at
        )
        VALUES (
            v_script_name,
            v_script_desc,
            v_user,
            NOW()
        )
        RETURNING script_id INTO v_script_id;
    END IF;

    -------------------------------------------------------------------
    -- 2. Registrar inicio de ejecución en dqm_exec_log
    -------------------------------------------------------------------
    INSERT INTO dqm_exec_log (script_id, started_at, status, message)
    VALUES (v_script_id, NOW(), 'RUNNING', 'Inicio actualización de Metadata Etapa 3.')
    RETURNING log_id INTO v_log_id;

    -------------------------------------------------------------------
    -- 3. Nuevas columnas country_key en dimensiones
    --    dim_customer, dim_supplier, dim_employee
    --    + relaciones con dim_country.country_key
    -------------------------------------------------------------------

    -- 3.1 dim_customer.country_key
    SELECT entity_id INTO v_entity_id
    FROM md_entities
    WHERE entity_name = 'dim_customer';

    IF v_entity_id IS NOT NULL THEN
        INSERT INTO md_attributes (
            entity_id, column_name, data_type,
            business_name, description,
            is_primary_key, is_foreign_key,
            referenced_entity, referenced_column,
            is_nullable, created_by, created_at
        )
        SELECT
            v_entity_id, 'country_key', 'INTEGER',
            'SK País', 'Clave foránea hacia dim_country (país del cliente).',
            FALSE, TRUE,
            'dim_country', 'country_key',
            TRUE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1
            FROM md_attributes a
            WHERE a.entity_id   = v_entity_id
              AND a.column_name = 'country_key'
        );
    END IF;

    -- Relación dim_country → dim_customer (country_key)
    INSERT INTO md_relationships (
        parent_entity, parent_column,
        child_entity,  child_column,
        relationship_type, description, created_by, created_at
    )
    SELECT
        'dim_country', 'country_key',
        'dim_customer', 'country_key',
        'FK',
        'Cada cliente se asocia a un país en dim_country mediante country_key.',
        v_user, NOW()
    WHERE NOT EXISTS (
        SELECT 1
        FROM md_relationships r
        WHERE r.parent_entity  = 'dim_country'
          AND r.parent_column  = 'country_key'
          AND r.child_entity   = 'dim_customer'
          AND r.child_column   = 'country_key'
    );


    -- 3.2 dim_supplier.country_key
    SELECT entity_id INTO v_entity_id
    FROM md_entities
    WHERE entity_name = 'dim_supplier';

    IF v_entity_id IS NOT NULL THEN
        INSERT INTO md_attributes (
            entity_id, column_name, data_type,
            business_name, description,
            is_primary_key, is_foreign_key,
            referenced_entity, referenced_column,
            is_nullable, created_by, created_at
        )
        SELECT
            v_entity_id, 'country_key', 'INTEGER',
            'SK País', 'Clave foránea hacia dim_country (país del proveedor).',
            FALSE, TRUE,
            'dim_country', 'country_key',
            TRUE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1
            FROM md_attributes a
            WHERE a.entity_id   = v_entity_id
              AND a.column_name = 'country_key'
        );
    END IF;

    -- Relación dim_country → dim_supplier
    INSERT INTO md_relationships (
        parent_entity, parent_column,
        child_entity,  child_column,
        relationship_type, description, created_by, created_at
    )
    SELECT
        'dim_country', 'country_key',
        'dim_supplier', 'country_key',
        'FK',
        'Cada proveedor se asocia a un país en dim_country mediante country_key.',
        v_user, NOW()
    WHERE NOT EXISTS (
        SELECT 1
        FROM md_relationships r
        WHERE r.parent_entity  = 'dim_country'
          AND r.parent_column  = 'country_key'
          AND r.child_entity   = 'dim_supplier'
          AND r.child_column   = 'country_key'
    );


    -- 3.3 dim_employee.country_key
    SELECT entity_id INTO v_entity_id
    FROM md_entities
    WHERE entity_name = 'dim_employee';

    IF v_entity_id IS NOT NULL THEN
        INSERT INTO md_attributes (
            entity_id, column_name, data_type,
            business_name, description,
            is_primary_key, is_foreign_key,
            referenced_entity, referenced_column,
            is_nullable, created_by, created_at
        )
        SELECT
            v_entity_id, 'country_key', 'INTEGER',
            'SK País', 'Clave foránea hacia dim_country (país del empleado).',
            FALSE, TRUE,
            'dim_country', 'country_key',
            TRUE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1
            FROM md_attributes a
            WHERE a.entity_id   = v_entity_id
              AND a.column_name = 'country_key'
        );
    END IF;

    -- Relación dim_country → dim_employee
    INSERT INTO md_relationships (
        parent_entity, parent_column,
        child_entity,  child_column,
        relationship_type, description, created_by, created_at
    )
    SELECT
        'dim_country', 'country_key',
        'dim_employee', 'country_key',
        'FK',
        'Cada empleado se asocia a un país en dim_country mediante country_key.',
        v_user, NOW()
    WHERE NOT EXISTS (
        SELECT 1
        FROM md_relationships r
        WHERE r.parent_entity  = 'dim_country'
          AND r.parent_column  = 'country_key'
          AND r.child_entity   = 'dim_employee'
          AND r.child_column   = 'country_key'
    );

    -------------------------------------------------------------------
    -- 4. Capa de MEMORIA (tablas DWM)
    --    - md_entities para cada tabla dwm_*
    --    - md_attributes para load_date y active
    -------------------------------------------------------------------
    -------------------------------------------------------------------
    -- dwm_categories
    IF NOT EXISTS (SELECT 1 FROM md_entities WHERE entity_name = 'dwm_categories') THEN
        INSERT INTO md_entities (
            entity_name, business_name, layer, entity_type,
            grain, primary_key, description,
            is_active, created_by, created_at
        )
        VALUES (
            'dwm_categories',
            'Memoria - Categorías',
            'DWM',
            'TABLE',
            'Una fila por categoría (histórica, activa/inactiva).',
            'category_id',
            'Tabla de memoria para categorías proveniente de TMP (Ingesta2).',
            TRUE, v_user, NOW()
        );
    END IF;

    -- dwm_suppliers
    IF NOT EXISTS (SELECT 1 FROM md_entities WHERE entity_name = 'dwm_suppliers') THEN
        INSERT INTO md_entities (
            entity_name, business_name, layer, entity_type,
            grain, primary_key, description,
            is_active, created_by, created_at
        )
        VALUES (
            'dwm_suppliers',
            'Memoria - Proveedores',
            'DWM',
            'TABLE',
            'Una fila por proveedor (histórica, activa/inactiva).',
            'supplier_id',
            'Tabla de memoria para proveedores proveniente de TMP (Ingesta2).',
            TRUE, v_user, NOW()
        );
    END IF;

    -- dwm_products
    IF NOT EXISTS (SELECT 1 FROM md_entities WHERE entity_name = 'dwm_products') THEN
        INSERT INTO md_entities (
            entity_name, business_name, layer, entity_type,
            grain, primary_key, description,
            is_active, created_by, created_at
        )
        VALUES (
            'dwm_products',
            'Memoria - Productos',
            'DWM',
            'TABLE',
            'Una fila por producto (histórica, activa/inactiva).',
            'product_id',
            'Tabla de memoria para productos proveniente de TMP (Ingesta2).',
            TRUE, v_user, NOW()
        );
    END IF;

    -- dwm_customers
    IF NOT EXISTS (SELECT 1 FROM md_entities WHERE entity_name = 'dwm_customers') THEN
        INSERT INTO md_entities (
            entity_name, business_name, layer, entity_type,
            grain, primary_key, description,
            is_active, created_by, created_at
        )
        VALUES (
            'dwm_customers',
            'Memoria - Clientes',
            'DWM',
            'TABLE',
            'Una fila por cliente (histórica, activa/inactiva).',
            'customer_id',
            'Tabla de memoria para clientes proveniente de TMP (Ingesta2).',
            TRUE, v_user, NOW()
        );
    END IF;

    -- dwm_employees
    IF NOT EXISTS (SELECT 1 FROM md_entities WHERE entity_name = 'dwm_employees') THEN
        INSERT INTO md_entities (
            entity_name, business_name, layer, entity_type,
            grain, primary_key, description,
            is_active, created_by, created_at
        )
        VALUES (
            'dwm_employees',
            'Memoria - Empleados',
            'DWM',
            'TABLE',
            'Una fila por empleado (histórica, activa/inactiva).',
            'employee_id',
            'Tabla de memoria para empleados proveniente de TMP (Ingesta2).',
            TRUE, v_user, NOW()
        );
    END IF;

    -- dwm_orders
    IF NOT EXISTS (SELECT 1 FROM md_entities WHERE entity_name = 'dwm_orders') THEN
        INSERT INTO md_entities (
            entity_name, business_name, layer, entity_type,
            grain, primary_key, description,
            is_active, created_by, created_at
        )
        VALUES (
            'dwm_orders',
            'Memoria - Órdenes',
            'DWM',
            'TABLE',
            'Una fila por orden (histórica, activa/inactiva).',
            'order_id',
            'Tabla de memoria para órdenes proveniente de TMP (Ingesta2).',
            TRUE, v_user, NOW()
        );
    END IF;

    -- dwm_order_details
    IF NOT EXISTS (SELECT 1 FROM md_entities WHERE entity_name = 'dwm_order_details') THEN
        INSERT INTO md_entities (
            entity_name, business_name, layer, entity_type,
            grain, primary_key, description,
            is_active, created_by, created_at
        )
        VALUES (
            'dwm_order_details',
            'Memoria - Detalle de órdenes',
            'DWM',
            'TABLE',
            'Una fila por detalle de orden (order_id + product_id).',
            'order_id, product_id',
            'Tabla de memoria para detalle de órdenes proveniente de TMP (Ingesta2).',
            TRUE, v_user, NOW()
        );
    END IF;

    -- dwm_shippers
    IF NOT EXISTS (SELECT 1 FROM md_entities WHERE entity_name = 'dwm_shippers') THEN
        INSERT INTO md_entities (
            entity_name, business_name, layer, entity_type,
            grain, primary_key, description,
            is_active, created_by, created_at
        )
        VALUES (
            'dwm_shippers',
            'Memoria - Shippers',
            'DWM',
            'TABLE',
            'Una fila por shipper (histórica, activa/inactiva).',
            'shipper_id',
            'Tabla de memoria para shippers proveniente de TMP (Ingesta2).',
            TRUE, v_user, NOW()
        );
    END IF;

    -- dwm_region
    IF NOT EXISTS (SELECT 1 FROM md_entities WHERE entity_name = 'dwm_region') THEN
        INSERT INTO md_entities (
            entity_name, business_name, layer, entity_type,
            grain, primary_key, description,
            is_active, created_by, created_at
        )
        VALUES (
            'dwm_region',
            'Memoria - Regiones',
            'DWM',
            'TABLE',
            'Una fila por región.',
            'region_id',
            'Tabla de memoria para regiones proveniente de TMP (Ingesta2).',
            TRUE, v_user, NOW()
        );
    END IF;

    -- dwm_territories
    IF NOT EXISTS (SELECT 1 FROM md_entities WHERE entity_name = 'dwm_territories') THEN
        INSERT INTO md_entities (
            entity_name, business_name, layer, entity_type,
            grain, primary_key, description,
            is_active, created_by, created_at
        )
        VALUES (
            'dwm_territories',
            'Memoria - Territorios',
            'DWM',
            'TABLE',
            'Una fila por territorio.',
            'territory_id',
            'Tabla de memoria para territorios proveniente de TMP (Ingesta2).',
            TRUE, v_user, NOW()
        );
    END IF;

    -- dwm_employee_territories
    IF NOT EXISTS (SELECT 1 FROM md_entities WHERE entity_name = 'dwm_employee_territories') THEN
        INSERT INTO md_entities (
            entity_name, business_name, layer, entity_type,
            grain, primary_key, description,
            is_active, created_by, created_at
        )
        VALUES (
            'dwm_employee_territories',
            'Memoria - Relación Empleado/Territorio',
            'DWM',
            'TABLE',
            'Una fila por combinación empleado + territorio.',
            'employee_id, territory_id',
            'Tabla de memoria para relación empleado/territorio proveniente de TMP (Ingesta2).',
            TRUE, v_user, NOW()
        );
    END IF;

    -------------------------------------------------------------------
    -- 5. Atributos load_date y active en DWM 
    -------------------------------------------------------------------

  
    PERFORM 1; 

    -- Función inline: agrega atributo si no exist

    -- dwm_categories: load_date + active
    SELECT entity_id INTO v_entity_id
    FROM md_entities
    WHERE entity_name = 'dwm_categories';

    IF v_entity_id IS NOT NULL THEN
        -- load_date
        INSERT INTO md_attributes (
            entity_id, column_name, data_type,
            business_name, description,
            is_primary_key, is_foreign_key,
            referenced_entity, referenced_column,
            is_nullable, created_by, created_at
        )
        SELECT
            v_entity_id, 'load_date', 'TIMESTAMP',
            'Fecha de carga', 'Fecha y hora de carga/actualización del registro en DWM.',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes
            WHERE entity_id = v_entity_id
              AND column_name = 'load_date'
        );

        -- active
        INSERT INTO md_attributes (
            entity_id, column_name, data_type,
            business_name, description,
            is_primary_key, is_foreign_key,
            referenced_entity, referenced_column,
            is_nullable, created_by, created_at
        )
        SELECT
            v_entity_id, 'active', 'BOOLEAN',
            'Registro activo', 'Flag de ABM (TRUE = activo, FALSE = baja lógica).',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes
            WHERE entity_id = v_entity_id
              AND column_name = 'active'
        );
    END IF;

    -- Repetir el mismo patrón para las demás tablas DWM
    -- dwm_suppliers
    SELECT entity_id INTO v_entity_id
    FROM md_entities
    WHERE entity_name = 'dwm_suppliers';

    IF v_entity_id IS NOT NULL THEN
        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'load_date', 'TIMESTAMP',
            'Fecha de carga', 'Fecha y hora de carga/actualización del registro en DWM.',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'load_date'
        );

        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'active', 'BOOLEAN',
            'Registro activo', 'Flag de ABM (TRUE = activo, FALSE = baja lógica).',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'active'
        );
    END IF;

    -- dwm_products
    SELECT entity_id INTO v_entity_id
    FROM md_entities
    WHERE entity_name = 'dwm_products';

    IF v_entity_id IS NOT NULL THEN
        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'load_date', 'TIMESTAMP',
            'Fecha de carga', 'Fecha y hora de carga/actualización del registro en DWM.',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'load_date'
        );

        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'active', 'BOOLEAN',
            'Registro activo', 'Flag de ABM (TRUE = activo, FALSE = baja lógica).',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'active'
        );
    END IF;

    -- dwm_customers
    SELECT entity_id INTO v_entity_id
    FROM md_entities
    WHERE entity_name = 'dwm_customers';

    IF v_entity_id IS NOT NULL THEN
        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'load_date', 'TIMESTAMP',
            'Fecha de carga', 'Fecha y hora de carga/actualización del registro en DWM.',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'load_date'
        );

        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'active', 'BOOLEAN',
            'Registro activo', 'Flag de ABM (TRUE = activo, FALSE = baja lógica).',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'active'
        );
    END IF;

    -- dwm_employees
    SELECT entity_id INTO v_entity_id
    FROM md_entities
    WHERE entity_name = 'dwm_employees';

    IF v_entity_id IS NOT NULL THEN
        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'load_date', 'TIMESTAMP',
            'Fecha de carga', 'Fecha y hora de carga/actualización del registro en DWM.',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'load_date'
        );

        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'active', 'BOOLEAN',
            'Registro activo', 'Flag de ABM (TRUE = activo, FALSE = baja lógica).',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'active'
        );
    END IF;

    -- dwm_orders
    SELECT entity_id INTO v_entity_id
    FROM md_entities
    WHERE entity_name = 'dwm_orders';

    IF v_entity_id IS NOT NULL THEN
        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'load_date', 'TIMESTAMP',
            'Fecha de carga', 'Fecha y hora de carga/actualización del registro en DWM.',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'load_date'
        );

        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'active', 'BOOLEAN',
            'Registro activo', 'Flag de ABM (TRUE = activo, FALSE = baja lógica).',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'active'
        );
    END IF;

    -- dwm_order_details
    SELECT entity_id INTO v_entity_id
    FROM md_entities
    WHERE entity_name = 'dwm_order_details';

    IF v_entity_id IS NOT NULL THEN
        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'load_date', 'TIMESTAMP',
            'Fecha de carga', 'Fecha y hora de carga/actualización del registro en DWM.',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'load_date'
        );

        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'active', 'BOOLEAN',
            'Registro activo', 'Flag de ABM (TRUE = activo, FALSE = baja lógica).',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'active'
        );
    END IF;

    -- dwm_shippers
    SELECT entity_id INTO v_entity_id
    FROM md_entities
    WHERE entity_name = 'dwm_shippers';

    IF v_entity_id IS NOT NULL THEN
        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'load_date', 'TIMESTAMP',
            'Fecha de carga', 'Fecha y hora de carga/actualización del registro en DWM.',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'load_date'
        );

        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'active', 'BOOLEAN',
            'Registro activo', 'Flag de ABM (TRUE = activo, FALSE = baja lógica).',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'active'
        );
    END IF;

    -- dwm_region
    SELECT entity_id INTO v_entity_id
    FROM md_entities
    WHERE entity_name = 'dwm_region';

    IF v_entity_id IS NOT NULL THEN
        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'load_date', 'TIMESTAMP',
            'Fecha de carga', 'Fecha y hora de carga/actualización del registro en DWM.',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'load_date'
        );

        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'active', 'BOOLEAN',
            'Registro activo', 'Flag de ABM (TRUE = activo, FALSE = baja lógica).',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'active'
        );
    END IF;

    -- dwm_territories
    SELECT entity_id INTO v_entity_id
    FROM md_entities
    WHERE entity_name = 'dwm_territories';

    IF v_entity_id IS NOT NULL THEN
        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'load_date', 'TIMESTAMP',
            'Fecha de carga', 'Fecha y hora de carga/actualización del registro en DWM.',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'load_date'
        );

        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'active', 'BOOLEAN',
            'Registro activo', 'Flag de ABM (TRUE = activo, FALSE = baja lógica).',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'active'
        );
    END IF;

    -- dwm_employee_territories
    SELECT entity_id INTO v_entity_id
    FROM md_entities
    WHERE entity_name = 'dwm_employee_territories';

    IF v_entity_id IS NOT NULL THEN
        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'load_date', 'TIMESTAMP',
            'Fecha de carga', 'Fecha y hora de carga/actualización del registro en DWM.',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'load_date'
        );

        INSERT INTO md_attributes
        SELECT
            v_entity_id, 'active', 'BOOLEAN',
            'Registro activo', 'Flag de ABM (TRUE = activo, FALSE = baja lógica).',
            FALSE, FALSE, NULL, NULL,
            FALSE, v_user, NOW()
        WHERE NOT EXISTS (
            SELECT 1 FROM md_attributes WHERE entity_id = v_entity_id AND column_name = 'active'
        );
    END IF;

    -------------------------------------------------------------------
    -- 6. Cerrar el log de ejecución en DQM
    -------------------------------------------------------------------
    UPDATE dqm_exec_log
    SET finished_at   = NOW(),
        status        = 'OK',
        message       = 'Metadata actualizada para Etapa 3 (país + memoria DWM + country_key en dimensiones).',
        rows_processed = 0
    WHERE log_id = v_log_id;

END $$;
