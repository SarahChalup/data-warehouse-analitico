---------------------------------------------------------------
-- E3_09_metadata_country.sql
-- Actualiza la Metadata para incluir la dimensión de países
-- (dim_country) y sus relaciones con tablas TMP.
-- Etapa 3 – Actualización
-- Autora: Mariana
---------------------------------------------------------------

DO $$
DECLARE
    v_log_id     BIGINT;
    v_script_id  INT;
    v_entity_id  INT;   -- para guardar el id de dim_country en md_entities
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
        'E3_09_metadata_country.sql',
        'Agrega dim_country a la Metadata y define relaciones con tablas TMP.',
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
    -- 3. Registrar la entidad dim_country en md_entities
    -------------------------------------------------------------------
    INSERT INTO md_entities (
        entity_name,
        business_name,
        layer,
        entity_type,
        grain,
        primary_key,
        description,
        created_by
    )
    VALUES (
        'dim_country',
        'Dimensión de países',
        'DWA_DIM',
        'TABLE',
        'Una fila por país',
        'country_id',
        'Dimensión de países basada en la fuente externa World-Data-2023.',
        'Mariana'
    )
    RETURNING entity_id INTO v_entity_id;

    -------------------------------------------------------------------
    -- 4. Registrar los atributos de dim_country en md_attributes
    --    (ajustá tipos/nombres si cambiás la definición de la tabla)
    -------------------------------------------------------------------
    INSERT INTO md_attributes (
        entity_id, column_name, data_type,
        business_name, description,
        is_primary_key, is_foreign_key,
        referenced_entity, referenced_column,
        is_nullable, created_by
    )
    VALUES
        (v_entity_id, 'country_id',      'SERIAL',
         'Identificador de país',
         'Clave surrogate de la dimensión de países.', 
         TRUE, FALSE, NULL, NULL, FALSE, 'Mariana'),

        (v_entity_id, 'country_name',    'VARCHAR(200)',
         'Nombre del país',
         'Nombre del país según World-Data-2023.',
         FALSE, FALSE, NULL, NULL, FALSE, 'Mariana'),

        (v_entity_id, 'country_code',    'VARCHAR(10)',
         'Código de país',
         'Código ISO u otro identificador del país.',
         FALSE, FALSE, NULL, NULL, TRUE, 'Mariana'),

        (v_entity_id, 'region',          'VARCHAR(200)',
         'Región',
         'Región geográfica del país (ej. Europe, Americas).',
         FALSE, FALSE, NULL, NULL, TRUE, 'Mariana'),

        (v_entity_id, 'subregion',       'VARCHAR(200)',
         'Subregión',
         'Subregión geográfica del país.',
         FALSE, FALSE, NULL, NULL, TRUE, 'Mariana'),

        (v_entity_id, 'income_group',    'VARCHAR(200)',
         'Grupo de ingresos',
         'Clasificación del país por nivel de ingresos.',
         FALSE, FALSE, NULL, NULL, TRUE, 'Mariana'),

        (v_entity_id, 'population_2023', 'BIGINT',
         'Población 2023',
         'Cantidad de habitantes del país según World-Data-2023.',
         FALSE, FALSE, NULL, NULL, TRUE, 'Mariana'),

        (v_entity_id, 'source_system',   'VARCHAR(50)',
         'Sistema origen',
         'Fuente de datos de la dimensión de países.',
         FALSE, FALSE, NULL, NULL, TRUE, 'Mariana'),

        (v_entity_id, 'valid_from',      'DATE',
         'Vigencia desde',
         'Fecha desde la cual es válido el registro.',
         FALSE, FALSE, NULL, NULL, TRUE, 'Mariana'),

        (v_entity_id, 'valid_to',        'DATE',
         'Vigencia hasta',
         'Fecha hasta la cual es válido el registro.',
         FALSE, FALSE, NULL, NULL, TRUE, 'Mariana'),

        (v_entity_id, 'is_current',      'BOOLEAN',
         'Es registro actual',
         'Indica si el registro es la versión vigente.',
         FALSE, FALSE, NULL, NULL, TRUE, 'Mariana');

    -------------------------------------------------------------------
    -- 5. Registrar relaciones de dim_country con tablas TMP
    --    (integración de países en la capa TMP)
    -------------------------------------------------------------------
    INSERT INTO md_relationships (
        parent_entity, parent_column,
        child_entity,  child_column,
        relationship_type, description, created_by
    )
    VALUES
        -- Clientes TMP → País
        ('dim_country', 'country_name',
         'tmp_customers', 'country',
         'FK',
         'Cada cliente TMP se asocia a un país en dim_country por nombre de país.',
         'Mariana'),

        -- Proveedores TMP → País
        ('dim_country', 'country_name',
         'tmp_suppliers', 'country',
         'FK',
         'Cada proveedor TMP se asocia a un país en dim_country por nombre de país.',
         'Mariana'),

        -- Empleados TMP → País
        ('dim_country', 'country_name',
         'tmp_employees', 'country',
         'FK',
         'Cada empleado TMP se asocia a un país en dim_country por nombre de país.',
         'Mariana'),

        -- Órdenes TMP → País de envío
        ('dim_country', 'country_name',
         'tmp_orders', 'ship_country',
         'FK',
         'Cada orden TMP se asocia a un país de envío en dim_country por nombre de país.',
         'Mariana');

    -------------------------------------------------------------------
    -- 6. Cerrar el log de ejecución en DQM
    -------------------------------------------------------------------
    UPDATE dqm_exec_log
    SET finished_at   = NOW(),
        status        = 'OK',
        message       = 'Metadata actualizada: dim_country documentada y relaciones con TMP registradas.',
        rows_processed = 0
    WHERE log_id = v_log_id;

END $$;
