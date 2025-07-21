USE ROLE SYSADMIN;
-- USE DATABASE --
USE DATABASE SALES_DWH;
-- USE SCHEMAS --
USE SCHEMA COMMON_SALES;
copy into sales_dwh.common_sales.exchange_rate
from 
(
select 
    t.$1::date as exchange_dt,
    to_decimal(t.$2) as usd2usd,
    to_decimal(t.$3,12,10) as usd2eu,
    to_decimal(t.$4,12,10) as usd2can,
    to_decimal(t.$4,12,10) as usd2uk,
    to_decimal(t.$4,12,10) as usd2inr,
    to_decimal(t.$4,12,10) as usd2jp
from 
     @sales_dwh.source.my_internal_stg/exchange/exchange-rate.csv
     (file_format => 'sales_dwh.common_sales.my_csv_format') t
);