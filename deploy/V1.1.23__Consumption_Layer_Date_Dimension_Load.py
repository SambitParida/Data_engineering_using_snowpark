from snowflake.snowpark import Session, DataFrame, Window, CaseExpr
from snowflake.snowpark.functions import col, lit, row_number, rank, concat,hash, split, expr, count, min, max
from snowflake.snowpark.types import StructType, StructField, StringType
import snowflake.snowpark.modin.plugin
from dotenv import load_dotenv
import pandas as pd
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


def create_date_dim(all_sales_df, session) -> None:
    start_date = all_sales_df.select(min(col("order_dt")).alias("min_order_dt")).collect()[0].as_dict()['MIN_ORDER_DT']
    end_date = all_sales_df.select(max(col("order_dt")).alias("max_order_dt")).collect()[0].as_dict()['MAX_ORDER_DT']
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    print(type(date_range))
    print(date_range)

    date_dim = pd.DataFrame()
    date_dim['ORDER_DT']=date_range.date
    date_dim['ORDER_YEAR'] = date_range.year
    start_day_of_year = pd.to_datetime(start_date).dayofyear
    date_dim['DayCounter'] = date_range.dayofyear - start_day_of_year + 1
    date_dim['ORDER_MONTH'] = date_range.month
    date_dim['ORDER_QUARTER'] = date_range.quarter
    date_dim['ORDER_DAY'] = date_range.day
    date_dim['ORDER_DAYOFWEEK'] = date_range.dayofweek
    date_dim['ORDER_DAYNAME'] = date_range.strftime('%A')
    date_dim['ORDER_DAYOFMONTH'] = date_range.day
    date_dim['ORDER_WEEKDAY'] = date_dim['ORDER_DAYOFWEEK'].map({
           0: 'Weekday', 1: 'Weekday', 2: 'Weekday', 3: 'Weekday',
        4: 'Weekday', 5: 'Weekend', 6: 'Weekend'
        })
    
    print(date_dim)
    print(type(date_dim))

#    date_dim_df = date_dim.to_snowpark()

    print("Converting pandas dataframe to snowpark dataframe")
    date_dim_df = session.create_dataframe(date_dim)
    
    date_dim_df = date_dim_df.with_column("hash_key",
                                                  hash(col('order_dt')))
    existing_date_dim_df = session.sql("select HASH_KEY from sales_dwh.consumption.date_dim ") 
    date_dim_df = date_dim_df.join(existing_date_dim_df,existing_date_dim_df['HASH_KEY']==date_dim_df['HASH_KEY'],join_type='leftanti')

    date_dim_df = date_dim_df.selectExpr("sales_dwh.consumption.date_dim_seq.nextval as DATE_ID_PK", 
                                   "ORDER_DT  as ORDER_DT", 
                                   "ORDER_YEAR as ORDER_YEAR", 
                                   "ORDER_MONTH as ORDER_MONTH",                                          
                                   "ORDER_QUARTER as ORDER_QUARTER",                                                              
                                   "ORDER_DAY as ORDER_DAY",
                                   "ORDER_DAYOFWEEK as ORDER_DAYOFWEEK", 
                                   "ORDER_DAYNAME as ORDER_DAYNAME", 
                                   "ORDER_DAYOFMONTH as ORDER_DAYOFMONTH",
                                   "ORDER_WEEKDAY as ORDER_WEEKDAY", 
                                   "HASH_KEY as HASH_KEY"
                                    )  
                                    
    insert_cnt = int(date_dim_df.count())
    if insert_cnt>0:
        date_dim_df.write.save_as_table("sales_dwh.consumption.date_dim",mode="append")
        print("Load data to date dimension table")
    else:
        print("No Insert.")


def main():
        session = get_snowpark_session()
        all_sales_query = """select * from curated.fr_sales_order
                            union
                            select * from curated.in_sales_order
                            union
                            select * from curated.us_sales_order
                        """
        sales_df = session.sql(all_sales_query)
        
        create_date_dim(sales_df,session)  
        session.close()
                                

if __name__ == '__main__':
    main()


