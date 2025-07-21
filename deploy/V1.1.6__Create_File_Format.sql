USE ROLE SYSADMIN;
-- USE DATABASE --
USE DATABASE SALES_DWH;
-- CREATE SCHEMAS --

USE SCHEMA common;
create or replace file format csv_format
    type = csv, 
    field_delimiter = ',',
    skip_header = 1,
    null_if = ('null','NULL',''),
    empty_field_as_null = true,
    field_optionally_enclosed_by = '\042'
    compression= auto;

create or replace file format my_json_format
    type = json 
    strip_outer_array = true
    compression = auto;

create or replace file format my_parquet_format
    type = parquet 
    compression = snappy;
