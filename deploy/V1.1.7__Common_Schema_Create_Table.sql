USE ROLE SYSADMIN;
-- USE DATABASE --
USE DATABASE SALES_DWH;
-- CREATE SCHEMAS --

USE SCHEMA COMMON_SALES;

create or replace transient table exchange_rate(
    date date, 
    usd2usd decimal(10,7),
    usd2eu decimal(10,7),
    usd2can decimal(10,7),
    usd2uk decimal(10,7),
    usd2inr decimal(10,7),
    usd2jp decimal(10,7)
);