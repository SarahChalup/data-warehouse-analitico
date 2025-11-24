DO $$
DECLARE
    v_log_id BIGINT;
    v_script_id INT := 1;  -- ejemplo
   v_total_validated_rows BIGINT := 0;
    v_current_table_rows BIGINT;
	v_records RECORD;	
    v_msg text;
    v_detail text;
    v_hint text;
    v_context text;
    v_script_name TEXT :=  'E2_04_dwa_create_tables.sql';
BEGIN
  -- Registrar el script en el inventario
  SELECT script_id INTO v_script_id 
  FROM dqm_scripts_inventory WHERE script_name = v_script_name;

  -- Si no existe (es NULL), lo creamos
  IF v_script_id IS NULL THEN
    INSERT INTO dqm_scripts_inventory (script_name, description, created_by, created_at)
    VALUES (
      v_script_name, 
      'Crear tablas de dimensión y tabla de hechos',
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


        -- ===================================================================
      -- CREACIÓN DEL MODELO DIMENSIONAL - ESQUEMA COPO DE NIEVE (SNOWFLAKE)
      -- ===================================================================


      -- Dimension: CATEGORIA
      CREATE TABLE dim_category (
          category_key     SERIAL PRIMARY KEY,   -- Surrogate Key
          category_id      INTEGER,              -- Business Key (Original)
          category_name    VARCHAR(50),
          load_date        TIMESTAMP DEFAULT NOW()
      );

      -- Dimension: PROVEEDOR (Supplier)
      CREATE TABLE dim_supplier (
          supplier_key     SERIAL PRIMARY KEY,   -- Surrogate Key
          supplier_id      INTEGER,              -- Business Key (Original)
          company_name     VARCHAR(100),
          load_date        TIMESTAMP DEFAULT NOW()
      );

      -- Dimension: PRODUCTO
      CREATE TABLE dim_product (
          product_key      SERIAL PRIMARY KEY,
          product_id       INTEGER,              -- Business Key
          product_name     VARCHAR(100),
          
          -- RELACIONES SNOWFLAKE (FK hacia otras dimensiones)
          category_key     INTEGER REFERENCES dim_category(category_key),
          supplier_key     INTEGER REFERENCES dim_supplier(supplier_key),
          
          discontinued     BOOLEAN,
          load_date        TIMESTAMP DEFAULT NOW()
      );

      -- Dimension: CLIENTE
      CREATE TABLE dim_customer (
          customer_key     SERIAL PRIMARY KEY,
          customer_id      VARCHAR(10),          -- Business Key
          company_name     VARCHAR(100),
          city             VARCHAR(50),
          country          VARCHAR(50),
          region           VARCHAR(50),
          load_date        TIMESTAMP DEFAULT NOW()
      );

      -- Dimension: EMPLEADO
      CREATE TABLE dim_employee (
          employee_key     SERIAL PRIMARY KEY,
          employee_id      INTEGER,              -- Business Key
          full_name        VARCHAR(100),
          title            VARCHAR(100),
          city             VARCHAR(50),
          country          VARCHAR(50),
          region           VARCHAR(50),
          hire_date        DATE,
          load_date        TIMESTAMP DEFAULT NOW()
      );

      -- Dimension: TIEMPO
      CREATE TABLE dim_time (
          date_key         INTEGER PRIMARY KEY,  -- Ej: 20231124
          full_date        DATE,
          year             INTEGER,
          month            INTEGER,
          quarter          VARCHAR(2),
          day              INTEGER,
          is_weekend       BOOLEAN
      );

      -- -------------------------------------------------------------------
      -- 4. TABLA DE HECHOS (FACT TABLE)
      -- -------------------------------------------------------------------

      CREATE TABLE fact_table (
          fact_key         SERIAL PRIMARY KEY,
          
          -- LLAVES FORÁNEAS (Apuntan a las Dimensions)
          product_key      INTEGER REFERENCES dim_product(product_key),
          customer_key     INTEGER REFERENCES dim_customer(customer_key),
          employee_key     INTEGER REFERENCES dim_employee(employee_key),
          date_key         INTEGER REFERENCES dim_time(date_key),
          
          -- MÉTRICAS (Facts)
          quantity         INTEGER,
          unit_price       NUMERIC(10,2),
          discount         NUMERIC(4,2),
          
          -- DATO DERIVADO (Enriquecimiento)
          total_amount     NUMERIC(12,2),
          
          load_date        TIMESTAMP DEFAULT NOW()
      );


-- ===================================================================
-- POBLADO DE METADATA: ENTIDADES (CAPA DIMENSIONAL)
-- Registramos las tablas dim_ y fact_ del modelo Snowflake
-- ===================================================================

INSERT INTO md_entities (
    entity_name, 
    business_name, 
    layer, 
    entity_type, 
    grain, 
    primary_key, 
    description, 
    is_active, 
    created_by, 
    created_at
) VALUES 
-- 1. DIMENSION CATEGORIA (Externa)
(
    'dim_category', 
    'Dimensión Categoría', 
    'Dimensional', 
    'Dimension', 
    'Una categoría de productos', 
    'category_key', 
    'Maestro de categorías normalizado. Punta del copo de nieve.', 
    true, 
    'Data Architect', 
    NOW()
),

-- 2. DIMENSION PROVEEDOR (Externa)
(
    'dim_supplier', 
    'Dimensión Proveedor', 
    'Dimensional', 
    'Dimension', 
    'Un proveedor', 
    'supplier_key', 
    'Maestro de proveedores normalizado. Punta del copo de nieve.', 
    true, 
    'Data Architect', 
    NOW()
),

-- 3. DIMENSION PRODUCTO (Central)
(
    'dim_product', 
    'Dimensión Producto', 
    'Dimensional', 
    'Dimension', 
    'Un producto específico', 
    'product_key', 
    'Dimension central del producto. Se conecta con Category y Supplier (Snowflake).', 
    true, 
    'Data Architect', 
    NOW()
),

-- 4. DIMENSION CLIENTE
(
    'dim_customer', 
    'Dimensión Cliente', 
    'Dimensional', 
    'Dimension', 
    'Un cliente único', 
    'customer_key', 
    'Maestro de clientes con datos geográficos.', 
    true, 
    'Data Architect', 
    NOW()
),

-- 5. DIMENSION EMPLEADO
(
    'dim_employee', 
    'Dimensión Empleado', 
    'Dimensional', 
    'Dimension', 
    'Un empleado', 
    'employee_key', 
    'Maestro de empleados con nombre completo y cargos.', 
    true, 
    'Data Architect', 
    NOW()
),

-- 6. DIMENSION TIEMPO
(
    'dim_time', 
    'Dimensión Tiempo', 
    'Dimensional', 
    'Dimension', 
    'Un día calendario', 
    'date_key', 
    'Dimension generada para navegación temporal (Año, Mes, Día, Trimestre).', 
    true, 
    'Data Architect', 
    NOW()
),

-- 7. TABLA DE HECHOS (FACT TABLE)
(
    'fact_table', 
    'Hechos Ventas', 
    'Dimensional', 
    'Fact', 
    'Una línea de detalle de un pedido', 
    'fact_key', 
    'Tabla central con métricas de venta (cantidad, precio, total) y claves foráneas.', 
    true, 
    'Data Architect', 
    NOW()
);



-- 1. ATRIBUTOS DE DIM_CATEGORY
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_category'), 'category_key', 'SERIAL', 'SK Categoría', 'Clave subrogada (interna DW).', true, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_category'), 'category_id', 'INTEGER', 'ID Negocio Categoría', 'ID original del sistema transaccional.', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_category'), 'category_name', 'VARCHAR(50)', 'Nombre Categoría', 'Nombre descriptivo.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_category'), 'load_date', 'TIMESTAMP', 'Fecha Carga', 'Auditoría.', false, false, null, null, false, 'Data Architect', NOW());

-- 2. ATRIBUTOS DE DIM_SUPPLIER
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_supplier'), 'supplier_key', 'SERIAL', 'SK Proveedor', 'Clave subrogada (interna DW).', true, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_supplier'), 'supplier_id', 'INTEGER', 'ID Negocio Proveedor', 'ID original del sistema transaccional.', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_supplier'), 'company_name', 'VARCHAR(100)', 'Nombre Proveedor', 'Razón social.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_supplier'), 'load_date', 'TIMESTAMP', 'Fecha Carga', 'Auditoría.', false, false, null, null, false, 'Data Architect', NOW());

-- 3. ATRIBUTOS DE DIM_PRODUCT (El centro del copo de nieve)
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_product'), 'product_key', 'SERIAL', 'SK Producto', 'Clave subrogada (interna DW).', true, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_product'), 'product_id', 'INTEGER', 'ID Negocio Producto', 'ID original del sistema transaccional.', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_product'), 'product_name', 'VARCHAR(100)', 'Nombre Producto', 'Nombre comercial.', false, false, null, null, true, 'Data Architect', NOW()),
-- Claves Foraneas Snowflake
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_product'), 'category_key', 'INTEGER', 'SK Categoría', 'FK hacia dimensión categoría (Snowflake).', false, true, 'dim_category', 'category_key', true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_product'), 'supplier_key', 'INTEGER', 'SK Proveedor', 'FK hacia dimensión proveedor (Snowflake).', false, true, 'dim_supplier', 'supplier_key', true, 'Data Architect', NOW()),
-- Atributos normales
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_product'), 'discontinued', 'BOOLEAN', 'Descontinuado', 'Indica si el producto sigue activo.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_product'), 'load_date', 'TIMESTAMP', 'Fecha Carga', 'Auditoría.', false, false, null, null, false, 'Data Architect', NOW());

-- 4. ATRIBUTOS DE DIM_CUSTOMER
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_customer'), 'customer_key', 'SERIAL', 'SK Cliente', 'Clave subrogada.', true, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_customer'), 'customer_id', 'VARCHAR(10)', 'ID Negocio Cliente', 'ID original.', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_customer'), 'company_name', 'VARCHAR(100)', 'Cliente', 'Nombre del cliente.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_customer'), 'city', 'VARCHAR(50)', 'Ciudad', 'Ubicación.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_customer'), 'country', 'VARCHAR(50)', 'País', 'Ubicación.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_customer'), 'region', 'VARCHAR(50)', 'Región', 'Ubicación.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_customer'), 'load_date', 'TIMESTAMP', 'Fecha Carga', 'Auditoría.', false, false, null, null, false, 'Data Architect', NOW());

-- 5. ATRIBUTOS DE DIM_EMPLOYEE
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_employee'), 'employee_key', 'SERIAL', 'SK Empleado', 'Clave subrogada.', true, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_employee'), 'employee_id', 'INTEGER', 'ID Negocio Empleado', 'ID original.', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_employee'), 'full_name', 'VARCHAR(100)', 'Nombre Completo', 'Concatenación nombre + apellido.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_employee'), 'title', 'VARCHAR(100)', 'Cargo', 'Puesto laboral.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_employee'), 'city', 'VARCHAR(50)', 'Ciudad', 'Ubicación.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_employee'), 'country', 'VARCHAR(50)', 'País', 'Ubicación.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_employee'), 'hire_date', 'DATE', 'Fecha Contratación', 'Fecha de ingreso.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_employee'), 'load_date', 'TIMESTAMP', 'Fecha Carga', 'Auditoría.', false, false, null, null, false, 'Data Architect', NOW());

-- 6. ATRIBUTOS DE DIM_TIME
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_time'), 'date_key', 'INTEGER', 'SK Fecha', 'Formato numérico YYYYMMDD.', true, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_time'), 'full_date', 'DATE', 'Fecha Completa', 'Fecha calendario estándar.', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_time'), 'year', 'INTEGER', 'Año', 'Año de cuatro dígitos.', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_time'), 'month', 'INTEGER', 'Mes', 'Número de mes (1-12).', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_time'), 'quarter', 'VARCHAR(2)', 'Trimestre', 'Q1, Q2, Q3, Q4.', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_time'), 'day', 'INTEGER', 'Día', 'Día del mes.', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dim_time'), 'is_weekend', 'BOOLEAN', 'Es Fin de Semana', 'Flag para sábados y domingos.', false, false, null, null, false, 'Data Architect', NOW());

-- 7. ATRIBUTOS DE FACT_TABLE
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'fact_table'), 'fact_key', 'SERIAL', 'PK Hechos', 'Identificador único de línea de hecho.', true, false, null, null, false, 'Data Architect', NOW()),
-- Llaves foráneas hacia el modelo estrella
((SELECT entity_id FROM md_entities WHERE entity_name = 'fact_table'), 'product_key', 'INTEGER', 'SK Producto', 'FK hacia Dimensión Producto.', false, true, 'dim_product', 'product_key', false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'fact_table'), 'customer_key', 'INTEGER', 'SK Cliente', 'FK hacia Dimensión Cliente.', false, true, 'dim_customer', 'customer_key', false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'fact_table'), 'employee_key', 'INTEGER', 'SK Empleado', 'FK hacia Dimensión Empleado.', false, true, 'dim_employee', 'employee_key', false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'fact_table'), 'date_key', 'INTEGER', 'SK Fecha', 'FK hacia Dimensión Tiempo.', false, true, 'dim_time', 'date_key', false, 'Data Architect', NOW()),
-- Métricas
((SELECT entity_id FROM md_entities WHERE entity_name = 'fact_table'), 'quantity', 'INTEGER', 'Cantidad', 'Unidades vendidas.', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'fact_table'), 'unit_price', 'NUMERIC(10,2)', 'Precio Unitario', 'Precio de venta al momento de la transacción.', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'fact_table'), 'discount', 'NUMERIC(4,2)', 'Descuento', 'Descuento aplicado.', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'fact_table'), 'total_amount', 'NUMERIC(12,2)', 'Monto Total', 'Dato Derivado: (Precio * Cantidad) - Descuento.', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'fact_table'), 'load_date', 'TIMESTAMP', 'Fecha Carga', 'Auditoría.', false, false, null, null, false, 'Data Architect', NOW());


-- ===================================================================
-- POBLADO DE METADATA: RELACIONES (CAPA DIMENSIONAL)
-- Documentamos las uniones del modelo Snowflake y Star Schema
-- ===================================================================


INSERT INTO md_relationships (
    parent_entity, 
    parent_column, 
    child_entity, 
    child_column, 
    relationship_type, 
    description, 
    created_by, 
    created_at
) VALUES 
-- -------------------------------------------------------------------
-- 1. RELACIONES "SNOWFLAKE" (Dimensión a Dimensión)
-- -------------------------------------------------------------------

-- Category -> Product
(
    'dim_category', 'category_key', 
    'dim_product', 'category_key', 
    'One-to-Many', 
    'Relación Snowflake: Una categoría agrupa múltiples productos.', 
    'Data Architect', NOW()
),

-- Supplier -> Product
(
    'dim_supplier', 'supplier_key', 
    'dim_product', 'supplier_key', 
    'One-to-Many', 
    'Relación Snowflake: Un proveedor suministra múltiples productos.', 
    'Data Architect', NOW()
),

-- -------------------------------------------------------------------
-- 2. RELACIONES "STAR SCHEMA" (Dimensión a Fact Table)
-- -------------------------------------------------------------------

-- Product -> Fact Sales
(
    'dim_product', 'product_key', 
    'fact_table', 'product_key', 
    'One-to-Many', 
    'Relación central: Vincula el producto vendido con el hecho de venta.', 
    'Data Architect', NOW()
),

-- Customer -> Fact Sales
(
    'dim_customer', 'customer_key', 
    'fact_table', 'customer_key', 
    'One-to-Many', 
    'Relación central: Identifica al cliente que realizó la compra.', 
    'Data Architect', NOW()
),

-- Employee -> Fact Sales
(
    'dim_employee', 'employee_key', 
    'fact_table', 'employee_key', 
    'One-to-Many', 
    'Relación central: Identifica al empleado responsable de la venta.', 
    'Data Architect', NOW()
),

-- Time -> Fact Sales
(
    'dim_time', 'date_key', 
    'fact_table', 'date_key', 
    'One-to-Many', 
    'Relación central: Sitúa la venta en un momento específico del tiempo.', 
    'Data Architect', NOW()
);







    -- =======================================================================    
    -- FIN DEL PROCESO
    -- =======================================================================    
-- Si esta sección finaliza sin error, actualiza el log como 'OK'
        UPDATE dqm_exec_log
        SET finished_at = NOW(),
            status = 'OK',
            message = 'Completado exitosamente.',
            rows_processed = 0 -- El valor acumulado
        WHERE log_id = v_log_id;

     RAISE NOTICE 'Script finalizado exitosamente sin errores.';
  
    -- ==========================================================
    -- 3. MANEJO DE EXCEPCIONES
    -- ==========================================================
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