-- Replace each bronze_count literal with the current count from S3 object manifests or staging metadata.

with warehouse_sales as (
    select count(*) as warehouse_count from retail_data.sales
),
expected_sales as (
    select 4 as bronze_count
)
select
    expected_sales.bronze_count,
    warehouse_sales.warehouse_count,
    warehouse_sales.warehouse_count - expected_sales.bronze_count as variance
from warehouse_sales
cross join expected_sales;

select
    sum(case when product_id is null then 1 else 0 end) as null_product_id,
    sum(case when customer_id is null then 1 else 0 end) as null_customer_id,
    sum(case when sales_amount is null then 1 else 0 end) as null_sales_amount
from retail_data.sales;

select *
from retail_data.sales
where sales_amount < 0
   or quantity <= 0
   or unit_price < 0;

select
    date_trunc('day', sale_date) as sale_day,
    branch_id,
    sum(sales_amount) as total_sales_amount,
    sum(quantity) as total_units_sold
from retail_data.sales
group by 1, 2
order by 1, 2;
