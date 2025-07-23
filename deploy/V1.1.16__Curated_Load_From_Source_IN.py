from snowflake.snowpark import Session
from dotenv import load_dotenv
import os
import sys
import logging

# initiate logging at info level

#logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt = '%I:%M:%S' ) 

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

def ingest_in_sales(session) -> None:
     session.sql(" \
                 copy into sales_dwh.source.in_sales_order from ( \
    select \
        sales_dwh.source.in_sales_order_seq.nextval as SALES_ORDER_KEY,\
        t.$1 as ORDER_ID,\
        t.$2 as CUSTOMER_NAME,\
        t.$3 as MOBILE_KEY,\
        t.$4 as ORDER_QUENTITY,\
        t.$5 as UNIT_PRICE,\
        t.$6 as ORDER_VALUE,\
        t.$7 as PROMOTION_CODE,\
        t.$8 as FINAL_ORDER_AMOUNT,\
        t.$9 as tax_amount,\
        t.$10 as ORDER_DT,\
        t.$11 as PAYMENT_STATUS,\
        t.$12 as SHIPPING_STATUS,\
        t.$13 as payment_method,\
        t.$14 as PAYMENT_PROVIDER,\
        t.$15 as MOBILE,\
        t.$16 as SHIPPING_ADDRESS,\
        metadata$filename as STG_FILE_NAME,\
        metadata$file_row_number as STG_ROW_NUMBER,\
        metadata$file_last_modified as STG_LAST_MODIFIED\
    from \
    @sales_dwh.source.MY_INTERNAL_STG/sales/source=IN/format=csv/\
    (\
        file_format => 'sales_dwh.common_sales.csv_format'\
    ) t\
) on_error = 'ABORT_STATEMENT'" \
                
     ).collect()


def main():
    session = get_snowpark_session()

    print ("<india sales order> : Before Copy")
    session.sql("select count (*) from sales_dwh.source.in_sales_order").show()

    ingest_in_sales(session)

    print ("<india sales order> : After Copy")

    session.sql("select count (*) from sales_dwh.source.in_sales_order").show()

if __name__ == '__main__':
    main()


