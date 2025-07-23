from snowflake.snowpark import Session
from dotenv import load_dotenv
import os
import sys
import logging

# initiate logging at info level

#logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt = '%I:%M:%S' ) 
query = """copy into sales_dwh.source.us_sales_order from ( 
    select 
        sales_dwh.source.us_sales_order_seq.nextval as SALES_ORDER_KEY,
        $1:"Order ID"::text as order_id,                   
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
    @sales_dwh.source.MY_INTERNAL_STG/sales/source=US/format=parquet/
    (
        file_format => 'sales_dwh.common_sales.my_parquet_format'
    ) t
) on_error = 'ABORT_STATEMENT' """

def get_snowpark_session() -> Session:
        load_dotenv()
        connection_parameters = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE")
        }
        return Session.builder.configs(connection_parameters).create()

def ingest_us_sales(session) -> None:
     session.sql(query                                      
     ).collect()
def main():
    session = get_snowpark_session()
    
    print ("<US sales order> : Before Copy")
    session.sql("select count (*) from sales_dwh.source.us_sales_order").show()

    ingest_us_sales(session)

    print ("<US sales order> : After Copy")

    session.sql("select count (*) from sales_dwh.source.us_sales_order").show()


if __name__ == '__main__':
    main()


