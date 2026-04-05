create schema if not exists retail_data;

create or replace table retail_data.products (
    product_id varchar,
    product_name varchar,
    category varchar,
    unit_price double,
    currency varchar,
    branch_id varchar,
    last_updated timestamp,
    source_file varchar
);

create or replace table retail_data.customers (
    customer_id varchar,
    first_name varchar,
    last_name varchar,
    email varchar,
    phone varchar,
    loyalty_tier varchar,
    branch_id varchar,
    created_at timestamp,
    source_file varchar
);

create or replace table retail_data.sales (
    sale_id varchar,
    sale_date timestamp,
    branch_id varchar,
    product_id varchar,
    customer_id varchar,
    quantity integer,
    unit_price double,
    sales_amount double,
    payment_method varchar,
    source_file varchar
);

create or replace table retail_data.inventory (
    inventory_id varchar,
    branch_id varchar,
    product_id varchar,
    on_hand_quantity integer,
    reorder_level integer,
    stock_status varchar,
    updated_at timestamp,
    source_file varchar
);
