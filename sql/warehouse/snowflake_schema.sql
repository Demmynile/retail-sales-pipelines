create schema if not exists retail_data;

create or replace table retail_data.products (
    product_id string,
    product_name string,
    category string,
    unit_price number(12, 2),
    currency string,
    branch_id string,
    last_updated timestamp_ntz,
    source_file string
);

create or replace table retail_data.customers (
    customer_id string,
    first_name string,
    last_name string,
    email string,
    phone string,
    loyalty_tier string,
    branch_id string,
    created_at timestamp_ntz,
    source_file string
);

create or replace table retail_data.sales (
    sale_id string,
    sale_date timestamp_ntz,
    branch_id string,
    product_id string,
    customer_id string,
    quantity number(10, 0),
    unit_price number(12, 2),
    sales_amount number(12, 2),
    payment_method string,
    source_file string
);

create or replace table retail_data.inventory (
    inventory_id string,
    branch_id string,
    product_id string,
    on_hand_quantity number(10, 0),
    reorder_level number(10, 0),
    stock_status string,
    updated_at timestamp_ntz,
    source_file string
);
