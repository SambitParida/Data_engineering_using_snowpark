USE ROLE SYSADMIN;
-- USE DATABASE --
USE DATABASE SALES_DWH;
USE SCHEMA SOURCE;
LIST @MY_INTERNAL_STG / SOURCE = IN;
SHOW FILE FORMATS;
DESC FILE FORMAT CSV_FORMAT;
---READ DATA FROM INTERNAL STAGE USING FILE FORMAT--
SELECT T.$1::TEXT AS ORDER_ID,
    T.$2::TEXT AS CUSTOMER_NAME,
    T.$3::TEXT AS MOBILE_KEY,
    T.$4::NUMBER AS ORDER_QUANTITY,
    T.$5::NUMBER AS UNIT_PRICE,
    T.$6::NUMBER AS ORDER_VALAUE,
    T.$7::TEXT AS PROMOTION_CODE,
    T.$8::NUMBER(10, 2) AS FINAL_ORDER_AMOUNT,
    T.$9::NUMBER(10, 2) AS TAX_AMOUNT,
    T.$10::DATE AS ORDER_DT,
    T.$11::TEXT AS PAYMENT_STATUS,
    T.$12::TEXT AS SHIPPING_STATUS,
    T.$13::TEXT AS PAYMENT_METHOD,
    T.$14::TEXT AS PAYMENT_PROVIDER,
    T.$15::TEXT AS MOBILE,
    T.$16::TEXT AS SHIPPING_ADDRESS
FROM @MY_INTERNAL_STG/sales/source=IN/format=csv/(FILE_FORMAT => 'SALES_DWH.COMMON_SALES.CSV_FORMAT') T;
-- INTERNAL STAGE - QUERY THE PARQUET DATA FILE FORMAT
SELECT $1 :"ORDER ID"::TEXT AS ORDE_ID,
    $1 :"CUSTOMER NAME"::TEXT AS CUSTOMER_NAME,
    $1 :"MOBILE MODEL"::TEXT AS MOBILE_KEY,
    TO_NUMBER($1 :"QUANTITY") AS QUANTITY,
    TO_NUMBER($1 :"PRICE PER UNIT") AS UNIT_PRICE,
    TO_DECIMAL($1 :"TOTAL PRICE") AS TOTAL_PRICE,
    $1 :"PROMOTION CODE"::TEXT AS PROMOTION_CODE,
    $1 :"ORDER AMOUNT"::NUMBER(10, 2) AS ORDER_AMOUNT,
    TO_DECIMAL($1 :"TAX") AS TAX,
    $1 :"ORDER DATE"::DATE AS ORDER_DT,
    $1 :"PAYMENT STATUS"::TEXT AS PAYMENT_STATUS,
    $1 :"SHIPPING STATUS"::TEXT AS SHIPPING_STATUS,
    $1 :"PAYMENT METHOD"::TEXT AS PAYMENT_METHOD,
    $1 :"PAYMENT PROVIDER"::TEXT AS PAYMENT_PROVIDER,
    $1 :"PHONE"::TEXT AS PHONE,
    $1 :"DELIVERY ADDRESS"::TEXT AS SHIPPING_ADDRESS
FROM @MY_INTERNAL_STG/SALES/ SOURCE = US / FORMAT = PARQUET / (
        FILE_FORMAT => 'SALES_DWH.SOURCE.MY_PARQUET_FORMAT'
    );
-- INTERNAL STAGE - QUERY THE JSON DATA FILE FORMAT
SELECT     $1:"Order ID"::text as orde_id,                   
    $1:"Customer Name"::text as customer_name,          
    $1:"Mobile Model"::text as mobile_key,              
    to_number($1:"Quantity") as quantity,               
    to_number($1:"Price per Unit") as unit_price,       
    to_decimal($1:"Total Price") as total_price,        
    $1:"Promotion Code"::text as promotion_code,        
    $1:"Order Amount"::number(10,2) as order_amount,    
    to_decimal($1:"Tax") as tax,                        
    $1:"Order Date"::date as order_dt,                  
    $1:"Payment Status"::text as payment_status,        
    $1:"Shipping Status"::text as shipping_status,      
    $1:"Payment Method"::text as payment_method,        
    $1:"Payment Provider"::text as payment_provider,    
    $1:"Phone"::text as phone,                          
    $1:"Delivery Address"::text as shipping_address
FROM @SALES_DWH.SOURCE.MY_INTERNAL_STG/sales/source=FR/format=json/(FILE_FORMAT => SALES_DWH.COMMON_SALES.MY_JSON_FORMAT);
SELECT T.$1::DATE AS EXCHANGE_DT,
    TO_DECIMAL(T.$2) AS USD2USD,
    TO_DECIMAL(T.$3, 18, 10) AS USD2EU,
    TO_DECIMAL(T.$4, 18, 10) AS USD2CAN,
    TO_DECIMAL(T.$5, 18, 10) AS USD2UK,
    TO_DECIMAL(T.$6, 18, 10) AS USD2INR,
    TO_DECIMAL(T.$7, 18, 10) AS USD2JP
FROM @SALES_DWH.SOURCE.MY_INTERNAL_STG/exchange-rate-data.csv  (
        FILE_FORMAT => 'SALES_DWH.COMMON_SALES.CSV_FORMAT'
    )T;


use schema common_sales;
SELECT *
FROM INFORMATION_SCHEMA.LOAD_HISTORY
WHERE TABLE_NAME = 'EXCHANGE_RATE'
ORDER BY LAST_LOAD_TIME DESC
LIMIT 10;


copy into sales_dwh.source.in_sales_order from (
    select
        in_sales_order_seq.nextval as sales_order_key,
        t.$1 as ORDER_ID,
        t.$2 as CUSTOMER_NAME,
        t.$3 as MOBILE_KEY,
        t.$4 as ORDER_QUENTITY,
        t.$5 as UNIT_PRICE,
        t.$6 as ORDER_VALUE,
        t.$7 as PROMOTION_CODE,
        t.$8 as FINAL_ORDER_AMOUNT,
        t.$9 as tax_amount,
        t.$10 as ORDER_DT,
        t.$11 as PAYMENT_STATUS,
        t.$12 as SHIPPING_STATUS,
        t.$13 as payment_method,
        t.$14 as PAYMENT_PROVIDER,
        t.$15 as MOBILE,
        t.$16 as SHIPPING_ADDRESS,
        metadata$filename as STG_FILE_NAME,
        metadata$file_row_number as STG_ROW_NUMBER,
        metadata$file_last_modified as STG_LAST_MODIFIED
    from 
    @sales_dwh.source.MY_INTERNAL_STG/sales/source=IN/format=csv/
    (
        file_format => 'sales_dwh.common_sales.csv_format'
    ) t
) on_error = 'ABORT_STATEMENT'


  select 
        sales_dwh.source.us_sales_order_seq.nextval as SALES_ORDER_KEY,
        $1:"Order ID"::text as orde_id,   
        $1:"Customer Name"::text as customer_name,
        $1:"Mobile Model"::text as mobile_key,
        to_number($1:"Quantity") as quantity,
        to_number($1:"Price per Unit") as unit_price,
        to_decimal($1:"Total Price") as total_price,
        $1:"Promotion Code"::text as promotion_code,
        $1:"Order Amount"::number(10,2) as order_amount,
        to_decimal($1:"Tax") as tax,
        $1:"Order Date"::date as order_dt,
        $1:"Payment Status"::text as payment_status,
        $1:"Shipping Status"::text as shipping_status,
        $1:"Payment Method"::text as payment_method,
        $1:"Payment Provider"::text as payment_provider,
        $1:"Phone"::text as phone,
        $1:"Delivery Address"::text as shipping_address,
        metadata$filename as STG_FILE_NAME,                 
        metadata$file_row_number as STG_ROW_NUMBER,         
        metadata$file_last_modified as STG_LAST_MODIFIED    
    from            
    @sales_dwh.source.MY_INTERNAL_STG/sales/source=FR/format=json/
    (
        file_format => 'sales_dwh.common_sales.my_json_format'
    ) t


copy into sales_dwh.source.fr_sales_order from ( 
    select 
        sales_dwh.source.fr_sales_order_seq.nextval as SALES_ORDER_KEY,
        $1:"Order ID"::text as orde_id,   
        $1:"Customer Name"::text as customer_name,
        $1:"Mobile Model"::text as mobile_key,
        to_number($1:"Quantity") as quantity,
        to_number($1:"Price per Unit") as unit_price,
        to_decimal($1:"Total Price") as total_price,
        $1:"Promotion Code"::text as promotion_code,
        $1:"Order Amount"::number(10,2) as order_amount,
        to_decimal($1:"Tax") as tax,
        $1:"Order Date"::date as order_dt,
        $1:"Payment Status"::text as payment_status,
        $1:"Shipping Status"::text as shipping_status,
        $1:"Payment Method"::text as payment_method,
        $1:"Payment Provider"::text as payment_provider,
        $1:"Phone"::text as phone,
        $1:"Delivery Address"::text as shipping_address,
        metadata$filename as STG_FILE_NAME,                 
        metadata$file_row_number as STG_ROW_NUMBER,         
        metadata$file_last_modified as STG_LAST_MODIFIED    
    from            
    @sales_dwh.source.MY_INTERNAL_STG/sales/source=FR/format=json/
    (
        file_format => 'sales_dwh.common_sales.my_json_format'
    ) t
) on_error = 'ABORT_STATEMENT' 