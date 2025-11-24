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
    v_script_name TEXT :=  'E2_02_dwm_create_tables.sql';
BEGIN
  -- Registrar el script en el inventario
  SELECT script_id INTO v_script_id 
  FROM dqm_scripts_inventory WHERE script_name = v_script_name;

  -- Si no existe (es NULL), lo creamos
  IF v_script_id IS NULL THEN
    INSERT INTO dqm_scripts_inventory (script_name, description, created_by, created_at)
    VALUES (
      v_script_name, 
      'Crear tablas para la capa de memoria',
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



        -- 1. dwm_CATEGORIES
        CREATE TABLE dwm_categories (
            category_id      INTEGER PRIMARY KEY,
            category_name    VARCHAR(50),
            description      TEXT,
            picture          BYTEA,
            load_date        TIMESTAMP DEFAULT NOW() -- Auditoría
        );

        -- 2. dwm_SUPPLIERS
        CREATE TABLE dwm_suppliers (
            supplier_id      INTEGER PRIMARY KEY,
            company_name     VARCHAR(100),
            contact_name     VARCHAR(100),
            contact_title    VARCHAR(100),
            address          VARCHAR(200),
            city             VARCHAR(50),
            region           VARCHAR(50),
            postal_code      VARCHAR(20),
            country          VARCHAR(50),
            phone            VARCHAR(50),
            fax              VARCHAR(50),
            homepage         TEXT,
            load_date        TIMESTAMP DEFAULT NOW()
        );

        -- 3. dwm_PRODUCTS
        CREATE TABLE dwm_products (
            product_id        INTEGER PRIMARY KEY,
            product_name      VARCHAR(100),
            supplier_id       INTEGER,
            category_id       INTEGER,
            quantity_per_unit VARCHAR(100),
            unit_price        NUMERIC(10,2),
            units_in_stock    INTEGER,
            units_on_order    INTEGER,
            reorder_level     INTEGER,
            discontinued      INTEGER,
            load_date         TIMESTAMP DEFAULT NOW()
        );

        -- 4. dwm_CUSTOMERS
        CREATE TABLE dwm_customers (
            customer_id       VARCHAR(10) PRIMARY KEY,
            company_name      VARCHAR(100),
            contact_name      VARCHAR(100),
            contact_title     VARCHAR(100),
            address           VARCHAR(200),
            city              VARCHAR(50),
            region            VARCHAR(50),
            postal_code       VARCHAR(20),
            country           VARCHAR(50),
            phone             VARCHAR(50),
            fax               VARCHAR(50),
            load_date         TIMESTAMP DEFAULT NOW()
        );



        -- 5. dwm_EMPLOYEES
        CREATE TABLE dwm_employees (
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
            photo_path        VARCHAR(200),
            load_date         TIMESTAMP DEFAULT NOW()
        );

        -- 6. dwm_ORDERS
        CREATE TABLE dwm_orders (
            order_id          INTEGER PRIMARY KEY,
            customer_id       VARCHAR(10),
            employee_id       INTEGER,
            order_date        DATE,
            required_date     DATE,
            shipped_date      DATE,
            ship_via          INTEGER,
            freight           NUMERIC(10,2),
            ship_name         VARCHAR(100),
            ship_address      VARCHAR(200),
            ship_city         VARCHAR(50),
            ship_region       VARCHAR(50),
            ship_postal_code  VARCHAR(20),
            ship_country      VARCHAR(50),
            load_date         TIMESTAMP DEFAULT NOW()
        );

        -- 7. dwm_ORDER_DETAILS
        CREATE TABLE dwm_order_details (
            order_id          INTEGER,
            product_id        INTEGER,
            unit_price        NUMERIC(10,2),
            quantity          INTEGER,
            discount          NUMERIC(4,2),
            PRIMARY KEY (order_id, product_id),
            load_date         TIMESTAMP DEFAULT NOW()
        );

        -- 8. dwm_SHIPPERS
        CREATE TABLE dwm_shippers (
            shipper_id        INTEGER PRIMARY KEY,
            company_name      VARCHAR(100),
            phone             VARCHAR(50),
            load_date         TIMESTAMP DEFAULT NOW()
        );

        -- 9. dwm_REGION
        CREATE TABLE dwm_region (
            region_id          INTEGER PRIMARY KEY,
            region_description VARCHAR(100),
            load_date          TIMESTAMP DEFAULT NOW()
        );

        -- 10. dwm_TERRITORIES
        CREATE TABLE dwm_territories (
            territory_id          VARCHAR(20) PRIMARY KEY,
            territory_description VARCHAR(100),
            region_id             INTEGER,
            load_date             TIMESTAMP DEFAULT NOW()
        );

        -- 11. dwm_EMPLOYEE_TERRITORIES
        CREATE TABLE dwm_employee_territories (
            employee_id       INTEGER,
            territory_id      VARCHAR(20),
            PRIMARY KEY (employee_id, territory_id),
            load_date         TIMESTAMP DEFAULT NOW()
        );

---------------------------------------------------------------------------
-- INSERTS EN TABLAS METADATA
---------------------------------------------------------------------------

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
        -- CATEGORIES
        (
            'dwm_categories', 
            'Categorías de Producto (Memoria)', 
            'Memory', 
            'Table', 
            'Una categoría de productos', 
            'category_id', 
            'Copia exacta de la tabla categories del sistema transaccional (Staging).', 
            true, 
            'Data Architect', 
            NOW()
        ),
        -- SUPPLIERS
        (
            'dwm_suppliers', 
            'Proveedores (Memoria)', 
            'Memory', 
            'Table', 
            'Un proveedor individual', 
            'supplier_id', 
            'Copia exacta de la tabla suppliers del sistema transaccional.', 
            true, 
            'Data Architect', 
            NOW()
        ),
        --  PRODUCTS
        (
            'dwm_products', 
            'Productos (Memoria)', 
            'Memory', 
            'Table', 
            'Un producto comercializable', 
            'product_id', 
            'Copia exacta de la tabla products. Contiene estado del inventario original.', 
            true, 
            'Data Architect', 
            NOW()
        ),
        -- CUSTOMERS
        (
            'dwm_customers', 
            'Clientes (Memoria)', 
            'Memory', 
            'Table', 
            'Un cliente registrado', 
            'customer_id', 
            'Copia exacta de la tabla customers.', 
            true, 
            'Data Architect', 
            NOW()
        ),
        --  EMPLOYEES
        (
            'dwm_employees', 
            'Empleados (Memoria)', 
            'Memory', 
            'Table', 
            'Un empleado de la empresa', 
            'employee_id', 
            'Copia exacta de la tabla employees.', 
            true, 
            'Data Architect', 
            NOW()
        ),
        --  ORDERS
        (
            'dwm_orders', 
            'Pedidos - Cabecera (Memoria)', 
            'Memory', 
            'Table', 
            'Un pedido realizado', 
            'order_id', 
            'Copia exacta de la tabla orders. Contiene datos de envío y fechas.', 
            true, 
            'Data Architect', 
            NOW()
        ),
        -- ORDER DETAILS
        (
            'dwm_order_details', 
            'Detalle de Pedidos (Memoria)', 
            'Memory', 
            'Table', 
            'Un item/producto dentro de un pedido', 
            'order_id, product_id', 
            'Copia exacta de order_details. Contiene precio, cantidad y descuento por ítem.', 
            true, 
            'Data Architect', 
            NOW()
        ),
        -- SHIPPERS
        (
            'dwm_shippers', 
            'Transportistas (Memoria)', 
            'Memory', 
            'Table', 
            'Una empresa de transporte', 
            'shipper_id', 
            'Copia exacta de la tabla shippers.', 
            true, 
            'Data Architect', 
            NOW()
        ),
        --  REGION
        (
            'dwm_region', 
            'Regiones (Memoria)', 
            'Memory', 
            'Table', 
            'Una región geográfica', 
            'region_id', 
            'Copia exacta de la tabla region.', 
            true, 
            'Data Architect', 
            NOW()
        ),
        -- TERRITORIES
        (
            'dwm_territories', 
            'Territorios (Memoria)', 
            'Memory', 
            'Table', 
            'Un territorio asignable', 
            'territory_id', 
            'Copia exacta de la tabla territories.', 
            true, 
            'Data Architect', 
            NOW()
        ),
        --  EMPLOYEE TERRITORIES
        (
            'dwm_employee_territories', 
            'Territorios de Empleados (Memoria)', 
            'Memory', 
            'Table', 
            'Asignación de territorio a empleado', 
            'employee_id, territory_id', 
            'Tabla puente que vincula empleados con territorios.', 
            true, 
            'Data Architect', 
            NOW()
        );


        -- ===================================================================
-- POBLADO DE METADATA: ATRIBUTOS (CAPA DE MEMORIA)
-- Registramos las columnas de las tablas dwm_
-- ===================================================================

-- 1. ATRIBUTOS DE DWM_CATEGORIES
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_categories'), 'category_id', 'INTEGER', 'ID Categoría', 'Identificador único de la categoría.', true, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_categories'), 'category_name', 'VARCHAR(50)', 'Nombre Categoría', 'Nombre descriptivo de la categoría.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_categories'), 'description', 'TEXT', 'Descripción', 'Detalle extenso de la categoría.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_categories'), 'picture', 'BYTEA', 'Imagen', 'Foto de la categoría en binario.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_categories'), 'load_date', 'TIMESTAMP', 'Fecha de Carga', 'Auditoría: Cuándo se insertó el dato.', false, false, null, null, false, 'Data Architect', NOW());

-- 2. ATRIBUTOS DE DWM_SUPPLIERS
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_suppliers'), 'supplier_id', 'INTEGER', 'ID Proveedor', 'Identificador único del proveedor.', true, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_suppliers'), 'company_name', 'VARCHAR(100)', 'Nombre Empresa', 'Razón social del proveedor.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_suppliers'), 'contact_name', 'VARCHAR(100)', 'Nombre Contacto', 'Persona de contacto principal.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_suppliers'), 'country', 'VARCHAR(50)', 'País', 'País de origen del proveedor.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_suppliers'), 'load_date', 'TIMESTAMP', 'Fecha de Carga', 'Auditoría de carga.', false, false, null, null, false, 'Data Architect', NOW())
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_suppliers'), 'contact_title', 'VARCHAR(100)', 'Cargo Contacto', 'Puesto laboral del contacto.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_suppliers'), 'address', 'VARCHAR(200)', 'Dirección', 'Calle y número.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_suppliers'), 'city', 'VARCHAR(50)', 'Ciudad', 'Ciudad del proveedor.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_suppliers'), 'region', 'VARCHAR(50)', 'Región', 'Estado o provincia.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_suppliers'), 'postal_code', 'VARCHAR(20)', 'Código Postal', 'CP del proveedor.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_suppliers'), 'phone', 'VARCHAR(50)', 'Teléfono', 'Teléfono de contacto.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_suppliers'), 'fax', 'VARCHAR(50)', 'Fax', 'Número de Fax.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_suppliers'), 'homepage', 'TEXT', 'Sitio Web', 'URL del sitio web del proveedor.', false, false, null, null, true, 'Data Architect', NOW());


-- 3. ATRIBUTOS DE DWM_PRODUCTS
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_products'), 'product_id', 'INTEGER', 'ID Producto', 'Identificador único del producto.', true, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_products'), 'product_name', 'VARCHAR(100)', 'Nombre Producto', 'Nombre comercial del producto.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_products'), 'supplier_id', 'INTEGER', 'ID Proveedor', 'FK hacia tabla de proveedores.', false, true, 'dwm_suppliers', 'supplier_id', true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_products'), 'category_id', 'INTEGER', 'ID Categoría', 'FK hacia tabla de categorías.', false, true, 'dwm_categories', 'category_id', true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_products'), 'unit_price', 'NUMERIC(10,2)', 'Precio Unitario', 'Precio de lista actual.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_products'), 'units_in_stock', 'INTEGER', 'Unidades en Stock', 'Cantidad física en almacén.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_products'), 'discontinued', 'INTEGER', 'Descontinuado', 'Flag (0/1) si el producto ya no se vende.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_products'), 'load_date', 'TIMESTAMP', 'Fecha de Carga', 'Auditoría de carga.', false, false, null, null, false, 'Data Architect', NOW());

-- 4. ATRIBUTOS DE DWM_CUSTOMERS
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_customers'), 'customer_id', 'VARCHAR(10)', 'ID Cliente', 'Código alfanumérico del cliente.', true, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_customers'), 'company_name', 'VARCHAR(100)', 'Empresa', 'Nombre de la empresa cliente.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_customers'), 'city', 'VARCHAR(50)', 'Ciudad', 'Ciudad del cliente.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_customers'), 'country', 'VARCHAR(50)', 'País', 'País del cliente.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_customers'), 'load_date', 'TIMESTAMP', 'Fecha de Carga', 'Auditoría de carga.', false, false, null, null, false, 'Data Architect', NOW())
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_customers'), 'contact_title', 'VARCHAR(100)', 'Cargo Contacto', 'Puesto laboral del contacto.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_customers'), 'address', 'VARCHAR(200)', 'Dirección', 'Calle y número.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_customers'), 'region', 'VARCHAR(50)', 'Región', 'Estado o provincia.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_customers'), 'postal_code', 'VARCHAR(20)', 'Código Postal', 'CP del cliente.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_customers'), 'phone', 'VARCHAR(50)', 'Teléfono', 'Teléfono de contacto.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_customers'), 'fax', 'VARCHAR(50)', 'Fax', 'Número de Fax.', false, false, null, null, true, 'Data Architect', NOW());

-- 5. ATRIBUTOS DE DWM_EMPLOYEES
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_employees'), 'employee_id', 'INTEGER', 'ID Empleado', 'Legajo del empleado.', true, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_employees'), 'last_name', 'VARCHAR(50)', 'Apellido', 'Apellido del empleado.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_employees'), 'first_name', 'VARCHAR(50)', 'Nombre', 'Nombre de pila del empleado.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_employees'), 'title', 'VARCHAR(100)', 'Cargo', 'Puesto laboral.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_employees'), 'reports_to', 'INTEGER', 'Supervisor', 'ID del jefe directo (recursivo).', false, true, 'dwm_employees', 'employee_id', true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_employees'), 'load_date', 'TIMESTAMP', 'Fecha de Carga', 'Auditoría de carga.', false, false, null, null, false, 'Data Architect', NOW());

-- 6. ATRIBUTOS DE DWM_ORDERS
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_orders'), 'order_id', 'INTEGER', 'Nro Orden', 'Número único de pedido.', true, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_orders'), 'customer_id', 'VARCHAR(10)', 'ID Cliente', 'Cliente que compró.', false, true, 'dwm_customers', 'customer_id', true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_orders'), 'employee_id', 'INTEGER', 'ID Vendedor', 'Empleado que vendió.', false, true, 'dwm_employees', 'employee_id', true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_orders'), 'order_date', 'DATE', 'Fecha Orden', 'Fecha en que se creó el pedido.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_orders'), 'ship_via', 'INTEGER', 'Transportista', 'ID de la empresa de envío.', false, true, 'dwm_shippers', 'shipper_id', true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_orders'), 'freight', 'NUMERIC(10,2)', 'Flete', 'Costo del envío.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_orders'), 'load_date', 'TIMESTAMP', 'Fecha de Carga', 'Auditoría de carga.', false, false, null, null, false, 'Data Architect', NOW())
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_orders'), 'required_date', 'DATE', 'Fecha Requerida', 'Fecha límite para entregar el pedido.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_orders'), 'shipped_date', 'DATE', 'Fecha Envío', 'Fecha real en la que se envió.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_orders'), 'ship_name', 'VARCHAR(100)', 'Nombre Destinatario', 'Nombre de la persona/empresa que recibe.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_orders'), 'ship_address', 'VARCHAR(200)', 'Dirección Envío', 'Calle y número de envío.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_orders'), 'ship_city', 'VARCHAR(50)', 'Ciudad Envío', 'Ciudad de destino.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_orders'), 'ship_region', 'VARCHAR(50)', 'Región Envío', 'Estado o provincia de destino.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_orders'), 'ship_postal_code', 'VARCHAR(20)', 'CP Envío', 'Código postal de destino.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_orders'), 'ship_country', 'VARCHAR(50)', 'País Envío', 'País de destino.', false, false, null, null, true, 'Data Architect', NOW());

-- 7. ATRIBUTOS DE DWM_ORDER_DETAILS
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_order_details'), 'order_id', 'INTEGER', 'Nro Orden', 'Parte de la PK compuesta. El pedido.', true, true, 'dwm_orders', 'order_id', false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_order_details'), 'product_id', 'INTEGER', 'ID Producto', 'Parte de la PK compuesta. El producto.', true, true, 'dwm_products', 'product_id', false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_order_details'), 'unit_price', 'NUMERIC(10,2)', 'Precio Venta', 'Precio al que se vendió (puede diferir del precio de lista).', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_order_details'), 'quantity', 'INTEGER', 'Cantidad', 'Unidades vendidas.', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_order_details'), 'discount', 'NUMERIC(4,2)', 'Descuento', 'Porcentaje de descuento aplicado (0 a 1).', false, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_order_details'), 'load_date', 'TIMESTAMP', 'Fecha de Carga', 'Auditoría de carga.', false, false, null, null, false, 'Data Architect', NOW());

-- 8. ATRIBUTOS DE DWM_SHIPPERS
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_shippers'), 'shipper_id', 'INTEGER', 'ID Transportista', 'Identificador único.', true, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_shippers'), 'company_name', 'VARCHAR(100)', 'Nombre Empresa', 'Nombre del transportista.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_shippers'), 'load_date', 'TIMESTAMP', 'Fecha de Carga', 'Auditoría de carga.', false, false, null, null, false, 'Data Architect', NOW());

-- 9. ATRIBUTOS DE DWM_REGION
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_region'), 'region_id', 'INTEGER', 'ID Región', 'Identificador de región.', true, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_region'), 'region_description', 'VARCHAR(100)', 'Descripción Región', 'Nombre de la región (Este, Oeste, etc).', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_region'), 'load_date', 'TIMESTAMP', 'Fecha de Carga', 'Auditoría de carga.', false, false, null, null, false, 'Data Architect', NOW());

-- 10. ATRIBUTOS DE DWM_TERRITORIES
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_territories'), 'territory_id', 'VARCHAR(20)', 'ID Territorio', 'Código postal o ID de territorio.', true, false, null, null, false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_territories'), 'territory_description', 'VARCHAR(100)', 'Descripción', 'Nombre del territorio.', false, false, null, null, true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_territories'), 'region_id', 'INTEGER', 'ID Región', 'FK a Región.', false, true, 'dwm_region', 'region_id', true, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_territories'), 'load_date', 'TIMESTAMP', 'Fecha de Carga', 'Auditoría de carga.', false, false, null, null, false, 'Data Architect', NOW());

-- 11. ATRIBUTOS DE DWM_EMPLOYEE_TERRITORIES
INSERT INTO md_attributes (entity_id, column_name, data_type, business_name, description, is_primary_key, is_foreign_key, referenced_entity, referenced_column, is_nullable, created_by, created_at)
VALUES 
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_employee_territories'), 'employee_id', 'INTEGER', 'ID Empleado', 'Parte de PK compuesta.', true, true, 'dwm_employees', 'employee_id', false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_employee_territories'), 'territory_id', 'VARCHAR(20)', 'ID Territorio', 'Parte de PK compuesta.', true, true, 'dwm_territories', 'territory_id', false, 'Data Architect', NOW()),
((SELECT entity_id FROM md_entities WHERE entity_name = 'dwm_employee_territories'), 'load_date', 'TIMESTAMP', 'Fecha de Carga', 'Auditoría de carga.', false, false, null, null, false, 'Data Architect', NOW());



        -- ===================================================================
        -- POBLADO DE METADATA: RELACIONES (CAPA DE MEMORIA)
        -- Documentamos las relaciones lógicas entre las tablas dwm_
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
        -- 1. Customers -> Orders (Un cliente hace muchos pedidos)
        (
            'dwm_customers', 'customer_id', 
            'dwm_orders', 'customer_id', 
            '1-a-N', 
            'Relación entre el maestro de clientes y sus pedidos realizados.', 
            'Data Architect', NOW()
        ),

        -- 2. Employees -> Orders (Un empleado gestiona muchos pedidos)
        (
            'dwm_employees', 'employee_id', 
            'dwm_orders', 'employee_id', 
            '1-a-N', 
            'Relación que identifica qué empleado atendió cada pedido.', 
            'Data Architect', NOW()
        ),

        -- 3. Shippers -> Orders (Un transportista envía muchos pedidos)
        -- Nota: En orders la columna se llama 'ship_via'
        (
            'dwm_shippers', 'shipper_id', 
            'dwm_orders', 'ship_via', 
            '1-a-N', 
            'Relación con la empresa de transporte encargada del envío.', 
            'Data Architect', NOW()
        ),

        -- 4. Orders -> Order Details (Un pedido tiene muchos detalles)
        (
            'dwm_orders', 'order_id', 
            'dwm_order_details', 'order_id', 
            '1-a-N', 
            'Relación cabecera-detalle. Vincula el pedido con sus ítems.', 
            'Data Architect', NOW()
        ),

        -- 5. Products -> Order Details (Un producto aparece en muchos detalles)
        (
            'dwm_products', 'product_id', 
            'dwm_order_details', 'product_id', 
            '1-a-N', 
            'Relación que identifica qué producto se vendió en cada línea de detalle.', 
            'Data Architect', NOW()
        ),

        -- 6. Suppliers -> Products (Un proveedor suministra muchos productos)
        (
            'dwm_suppliers', 'supplier_id', 
            'dwm_products', 'supplier_id', 
            '1-a-N', 
            'Relación que indica el proveedor de origen de cada producto.', 
            'Data Architect', NOW()
        ),

        -- 7. Categories -> Products (Una categoría agrupa muchos productos)
        (
            'dwm_categories', 'category_id', 
            'dwm_products', 'category_id', 
            '1-a-N', 
            'Clasificación de los productos por categoría.', 
            'Data Architect', NOW()
        ),

        -- 8. Employees -> Employees (Relación Recursiva: Jefe -> Subordinado)
        (
            'dwm_employees', 'employee_id', 
            'dwm_employees', 'reports_to', 
            'Recursive (1-a-N)', 
            'Jerarquía de empleados. Indica a quién reporta cada empleado.', 
            'Data Architect', NOW()
        ),

        -- 9. Region -> Territories (Una región tiene muchos territorios)
        (
            'dwm_region', 'region_id', 
            'dwm_territories', 'region_id', 
            '1-a-N', 
            'Agrupación geográfica de territorios por región.', 
            'Data Architect', NOW()
        ),

        -- 10. Employees -> Employee Territories (Relación Muchos a Muchos - Parte A)
        (
            'dwm_employees', 'employee_id', 
            'dwm_employee_territories', 'employee_id', 
            '1-a-N', 
            'Relación para asignar empleados a territorios (Tabla intermedia).', 
            'Data Architect', NOW()
        ),

        -- 11. Territories -> Employee Territories (Relación Muchos a Muchos - Parte B)
        (
            'dwm_territories', 'territory_id', 
            'dwm_employee_territories', 'territory_id', 
            '1-a-N', 
            'Relación para asignar territorios a empleados (Tabla intermedia).', 
            'Data Architect', NOW()
        ),

      
             SELECT (SELECT COUNT(*) FROM md_entities) +
                (SELECT COUNT(*) FROM md_attributes) +
                (SELECT COUNT(*) FROM md_relationships)
            INTO v_total_validated_rows;
        
-- =======================================================================    
-- Si esta sección finaliza sin error, actualiza el log como 'OK'
        UPDATE dqm_exec_log
        SET finished_at = NOW(),
            status = 'OK',
            message = 'Completado exitosamente.',
            rows_processed = v_total_validated_rows -- El valor acumulado
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