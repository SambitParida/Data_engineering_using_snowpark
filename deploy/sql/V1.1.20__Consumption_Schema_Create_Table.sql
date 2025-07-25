USE ROLE SYSADMIN;
-- USE DATABASE --
USE DATABASE SALES_DWH;
-- USE SCHEMAS --
-- Following are for consumption schema
-- -----------------------------------
use schema consumption;
create or replace  table region_dim(
    region_id_pk number primary key,
    Country text, 
    Region text,
    isActive text(1),
    hash_key number
);
create or replace  table product_dim(
    product_id_pk number primary key,
    Mobile_key text,
    Brand text, 
    Model text,
    Color text,
    Memory text,
    HDD text,
    isActive text(1),
    hash_key number
);

create or replace  table promo_code_dim(
    promo_code_id_pk number primary key,
    promo_code text,
    isActive text(1),
    hash_key number
);

create or replace  table customer_dim(
    customer_id_pk number primary key,
    customer_name text,
    CONCTACT_NO text,
    SHIPPING_ADDRESS text,
    country text,
    region text,
    isActive text(1),
    hash_key number
);

create or replace  table payment_dim(
    payment_id_pk number primary key,
    PAYMENT_METHOD text,
    PAYMENT_PROVIDER text,
    country text,
    region text,
    isActive text(1),
    hash_key number
);

create or replace  table date_dim(
    date_id_pk int primary key,
    order_dt date,
    order_year int,
    oder_month int,
    order_quater int,
    order_day int,
    order_dayofweek int,
    order_dayname text,
    order_dayofmonth int,
    order_weekday text,
    hash_key number
);

create or replace table sales_fact (
 order_id_pk number(38,0),
 order_code varchar(),
 date_id_fk number(38,0),
 region_id_fk number(38,0),
 customer_id_fk number(38,0),
 payment_id_fk number(38,0),
 product_id_fk number(38,0),
 promo_code_id_fk number(38,0),
 order_quantity number(38,0),
 local_total_order_amt number(10,2),
 local_tax_amt number(10,2),
 exhchange_rate number(15,7),
 us_total_order_amt number(23,8),
 usd_tax_amt number(23,8),
 hash_key number
);