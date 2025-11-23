---------------------------------------------------------------
-- E2_01_metadata_create.sql
-- Soporte de Metadata + descripción inicial de entidades TMP
---------------------------------------------------------------

DO $$
DECLARE
    v_log_id     BIGINT;
    v_script_id  INT;
    v_entity_id  INT;   -- para guardar el id de cada entidad en md_entities
BEGIN
    -------------------------------------------------------------------
    -- 1. Registrar este script en el inventario DQM
    -------------------------------------------------------------------
    INSERT INTO dqm_scripts_inventory (
        script_name,
        description,
        created_by,
        created_at
    )
    VALUES (
        'E2_01_metadata_create.sql',
        'Crea tablas de Metadata (entities, attributes, relationships) y describe entidades TMP',
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
    -- 3. CREACIÓN DE TABLAS DE METADATA
    -------------------------------------------------------------------

    -- 3.1. Metadata de entidades (tablas, vistas, etc.)
    --     Una fila por entidad lógica del DWA / TMP / TXT / DQM / METADATA
    CREATE TABLE IF NOT EXISTS md_entities (
        entity_id        SERIAL PRIMARY KEY,
        entity_name      VARCHAR(100) NOT NULL,   -- nombre físico: tmp_customers, dim_products, fact_orders
        business_name    VARCHAR(200),            -- nombre “de negocio”: Clientes, Productos, Ventas
        layer            VARCHAR(50),             -- TXT / TMP / DWA_DIM / DWA_FACT / DQM / METADATA
        entity_type      VARCHAR(50),             -- TABLE / VIEW / MATERIALIZED_VIEW
        grain            VARCHAR(200),            -- nivel de detalle (ej: "una fila por cliente")
        primary_key      VARCHAR(200),            -- nombre(s) de la PK (texto)
        description      TEXT,
        is_active        BOOLEAN DEFAULT TRUE,
        created_by       VARCHAR(50),
        created_at       TIMESTAMP DEFAULT NOW()
    );

    INSERT INTO dqm_object_inventory (object_name, object_type, created_by_script, created_at)
    VALUES ('md_entities', 'table', v_script_id, NOW())
    ON CONFLICT DO NOTHING;

    -- 3.2. Metadata de atributos/columnas
    --     Una fila por columna de cada entidad
    CREATE TABLE IF NOT EXISTS md_attributes (
        attribute_id        SERIAL PRIMARY KEY,
        entity_id           INT NOT NULL REFERENCES md_entities(entity_id),
        column_name         VARCHAR(100) NOT NULL,  -- nombre de la columna física
        data_type           VARCHAR(100),           -- tipo de dato (tal como está en la BD)
        business_name       VARCHAR(200),           -- nombre "de negocio"
        description         TEXT,
        is_primary_key      BOOLEAN DEFAULT FALSE,
        is_foreign_key      BOOLEAN DEFAULT FALSE,
        referenced_entity   VARCHAR(100),           -- nombre de la tabla referenciada (si es FK)
        referenced_column   VARCHAR(100),           -- columna referenciada (si es FK)
        is_nullable         BOOLEAN,
        created_by          VARCHAR(50),
        created_at          TIMESTAMP DEFAULT NOW()
    );

    INSERT INTO dqm_object_inventory (object_name, object_type, created_by_script, created_at)
    VALUES ('md_attributes', 'table', v_script_id, NOW())
    ON CONFLICT DO NOTHING;

    -- 3.3. Metadata de relaciones (FK entre entidades)
    --     Una fila por relación padre-hijo (normalmente una FK)
    CREATE TABLE IF NOT EXISTS md_relationships (
        relationship_id   SERIAL PRIMARY KEY,
        parent_entity     VARCHAR(100) NOT NULL,   -- tabla padre (PK)
        parent_column     VARCHAR(100) NOT NULL,   -- columna PK
        child_entity      VARCHAR(100) NOT NULL,   -- tabla hija (FK)
        child_column      VARCHAR(100) NOT NULL,   -- columna FK
        relationship_type VARCHAR(50) DEFAULT 'FK',-- FK / 1:N / N:1 / N:N
        description       TEXT,
        created_by        VARCHAR(50),
        created_at        TIMESTAMP DEFAULT NOW()
    );

    INSERT INTO dqm_object_inventory (object_name, object_type, created_by_script, created_at)
    VALUES ('md_relationships', 'table', v_script_id, NOW())
    ON CONFLICT DO NOTHING;

    -------------------------------------------------------------------
    -- 4. DESCRIPCIÓN INICIAL DE ALGUNAS ENTIDADES TMP
    -------------------------------------------------------------------
    -------------------------------------------------------------------
    -- 4.1. Registrar la entidad TMP_CUSTOMERS
    -------------------------------------------------------------------
    INSERT INTO md_entities (
        entity_name, business_name, layer, entity_type,
        grain, primary_key, description, created_by
    )
    VALUES (
        'tmp_customers',
        'Clientes (capa TMP)',
        'TMP',
        'TABLE',
        'Una fila por cliente',
        'customer_id',
        'Tabla temporal con clientes depurados desde TXT.',
        'Mariana'
    )
    RETURNING entity_id INTO v_entity_id;

    -- Registrar algunos atributos de TMP_CUSTOMERS
    INSERT INTO md_attributes (
        entity_id, column_name, data_type, business_name, description,
        is_primary_key, is_foreign_key, is_nullable, created_by
    )
    VALUES
        (v_entity_id, 'customer_id',   'VARCHAR(10)', 'Identificador de cliente',
         'Clave primaria del cliente (código de Northwind).', TRUE,  FALSE, FALSE, 'Mariana'),
        (v_entity_id, 'company_name',  'VARCHAR(100)', 'Nombre de la compañía',
         'Razón social del cliente.', FALSE, FALSE, TRUE, 'Mariana'),
        (v_entity_id, 'contact_name',  'VARCHAR(100)', 'Nombre de contacto',
         'Persona de contacto en la empresa cliente.', FALSE, FALSE, TRUE, 'Mariana'),
        (v_entity_id, 'city',          'VARCHAR(50)', 'Ciudad',
         'Ciudad del domicilio principal del cliente.', FALSE, FALSE, TRUE, 'Mariana'),
        (v_entity_id, 'country',       'VARCHAR(50)', 'País',
         'País del domicilio principal del cliente.', FALSE, FALSE, TRUE, 'Mariana');

    -------------------------------------------------------------------
    -- 4.2. Registrar la entidad TMP_ORDERS
    -------------------------------------------------------------------
    INSERT INTO md_entities (
        entity_name, business_name, layer, entity_type,
        grain, primary_key, description, created_by
    )
    VALUES (
        'tmp_orders',
        'Órdenes de venta (capa TMP)',
        'TMP',
        'TABLE',
        'Una fila por orden de venta',
        'order_id',
        'Órdenes de venta normalizadas a partir de TXT.',
        'Mariana'
    )
    RETURNING entity_id INTO v_entity_id;

    -- Registrar algunos atributos de TMP_ORDERS
    INSERT INTO md_attributes (
        entity_id, column_name, data_type, business_name, description,
        is_primary_key, is_foreign_key, referenced_entity, referenced_column,
        is_nullable, created_by
    )
    VALUES
        (v_entity_id, 'order_id',      'INTEGER', 'Identificador de orden',
         'Clave primaria de la orden.', TRUE, FALSE, NULL, NULL, FALSE, 'Mariana'),
        (v_entity_id, 'customer_id',   'VARCHAR(10)', 'Cliente',
         'Cliente asociado a la orden.', FALSE, TRUE, 'tmp_customers', 'customer_id', TRUE, 'Mariana'),
        (v_entity_id, 'employee_id',   'INTEGER', 'Empleado',
         'Vendedor responsable de la orden.', FALSE, TRUE, 'tmp_employees', 'employee_id', TRUE, 'Mariana'),
        (v_entity_id, 'order_date',    'DATE', 'Fecha de orden',
         'Fecha en la que se registró la orden.', FALSE, FALSE, NULL, NULL, TRUE, 'Mariana'),
        (v_entity_id, 'ship_via',      'INTEGER', 'Transportista',
         'Transportista seleccionado para el envío.', FALSE, TRUE, 'tmp_shippers', 'shipper_id', TRUE, 'Mariana');

    -------------------------------------------------------------------
    -- 4.3. Registrar algunas relaciones FK en md_relationships
    -------------------------------------------------------------------
    INSERT INTO md_relationships (
        parent_entity, parent_column,
        child_entity,  child_column,
        relationship_type, description, created_by
    )
    VALUES
        ('tmp_customers', 'customer_id',
         'tmp_orders',    'customer_id',
         'FK', 'Cada orden referencia un cliente en tmp_customers.', 'Mariana'),
        ('tmp_employees', 'employee_id',
         'tmp_orders',    'employee_id',
         'FK', 'Cada orden referencia un empleado vendedor en tmp_employees.', 'Mariana'),
        ('tmp_shippers',  'shipper_id',
         'tmp_orders',    'ship_via',
         'FK', 'Cada orden referencia un transportista en tmp_shippers.', 'Mariana');

    -------------------------------------------------------------------
    -- 5. Cerrar el log de ejecución en DQM
    -------------------------------------------------------------------
    UPDATE dqm_exec_log
    SET finished_at   = NOW(),
        status        = 'OK',
        message       = 'Metadata creada (entities, attributes, relationships) y entidades TMP documentadas parcialmente.',
        rows_processed = 0
    WHERE log_id = v_log_id;

END $$;
