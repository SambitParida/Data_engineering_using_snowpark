USE ROLE SYSADMIN;
-- USE DATABASE --
USE DATABASE SALES_DWH;
-- USE SCHEMAS --
-- Following are for consumption schema
-- -----------------------------------
use schema consumption;
create or replace sequence region_dim_seq start = 1 increment = 1;
create or replace sequence product_dim_seq start = 1 increment = 1;
create or replace sequence promo_code_dim_seq start = 1 increment = 1;
create or replace sequence customer_dim_seq start = 1 increment = 1;
create or replace sequence payment_dim_seq start = 1 increment = 1;
create or replace sequence date_dim_seq start = 1 increment = 1;
create or replace sequence sales_fact_seq start = 1 increment = 1;