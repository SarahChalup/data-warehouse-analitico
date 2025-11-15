CREATE TABLE txt_categories
(      
    category_id TEXT,
    category_name TEXT,
    description TEXT
);

CREATE TABLE txt_customers
(      
    customer_id TEXT,
    customer_name TEXT,
    contact_name TEXT,
    address TEXT,
    city TEXT,
    postal_code TEXT,
    country TEXT
);

CREATE TABLE txt_employee_territories
(
    employee_id TEXT,
    territory_id TEXT

);

CREATE TABLE txt_employees
(
    employee_id TEXT,
    last_name TEXT,
    first_name TEXT,
    birth_date DATE,
    photo TEXT,
    notes TEXT
);

CREATE TABLE txt_shippers(
    shipper_id TEXT,
    shipper_name TEXT,
    phone TEXT
);

CREATE TABLE txt_suppliers(
    supplier_id TEXT,
    supplier_name TEXT,
    contact_name TEXT,
    address TEXT,
    city TEXT,
    postal_code TEXT,
    country TEXT,
    phone TEXT
);

CREATE TABLE txt_products(
    product_id TEXT,
    product_name TEXT,
    supplier_id INTEGER,
    category_id INTEGER,
    unit TEXT,
    price TEXT
);

CREATE TABLE txt_orders(
    order_id TEXT,
    customer_id INTEGER,
    employee_id INTEGER,
    order_date DATETIME,
    shipper_id INTEGER
);

CREATE TABLE txt_order_details(
    order_detail_id TEXT,
    order_id TEXT,
    product_id TEXT,
    quantity TEXT
);

CREATE TABLE txt_regions
(
    region_id TEXT,
    region_description TEXT
);

CREATE TABLE txt_territories
(
    territory_id TEXT,
    territory_description TEXT,
    region_id TEXT
);