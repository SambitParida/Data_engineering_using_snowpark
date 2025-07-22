from snowflake.snowpark import Session
from dotenv import load_dotenv
import os
import sys
import logging

# initiate logging at info level

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt = '%I:%M:%S' ) 

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

def main():
    session = get_snowpark_session()

    context_df = session.sql("select current_role(), current_database(), current_schema(), current_warehouse()")
    context_df.show(2)

    #context_df = session.sql("select c_custkey,c_name,c_phone,c_mktsegment from snowflake_sample_data.tpch_sf1.customer limit 10")
    #context_df.show(5)

if __name__ == '__main__':
    main()


