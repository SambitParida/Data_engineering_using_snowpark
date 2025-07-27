from snowflake.snowpark import Session, DataFrame, Window, CaseExpr
from snowflake.snowpark.functions import col, lit, row_number, rank, concat,hash, split, expr, count
from snowflake.snowpark.types import StructType
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

def create_sales_fact(all_sales_df, session) -> None:
        date_dim_df = session.sql("select date_id_pk, order_dt from sales_dwh.consumption.date_dim")
        customer_dim_df = session.sql("select customer_id_pk, customer_name, country, region from sales_dwh.consumption.CUSTOMER_DIM")
        payment_dim_df = session.sql("select payment_id_pk, payment_method, payment_provider, country, region from sales_dwh.consumption.PAYMENT_DIM")
        product_dim_df = session.sql("select product_id_pk, mobile_key from sales_dwh.consumption.PRODUCT_DIM")
        promo_code_dim_df = session.sql("select promo_code_id_pk,promo_code as promotion_code,country, region from sales_dwh.consumption.PROMO_CODE_DIM")
        region_dim_df = session.sql("select region_id_pk,country, region from sales_dwh.consumption.REGION_DIM")

        print("count of data in date_dim_df is :", date_dim_df.count())
        print("count of data in customer_dim_df is :", customer_dim_df.count())
        print("count of data in payment_dim_df is :", payment_dim_df.count())
        print("count of data in product_dim_df is :", product_dim_df.count())
        print("count of data in promo_code_dim_df is :", promo_code_dim_df.count())
        print("count of data in region_dim_df is :", region_dim_df.count())
        
        all_sales_df = all_sales_df.join(date_dim_df, ["order_dt"],join_type='inner')
        all_sales_df = all_sales_df.join(customer_dim_df, ["customer_name","region","country"],join_type='inner')
        all_sales_df = all_sales_df.join(payment_dim_df, ["payment_method", "payment_provider", "country", "region"],join_type='inner')
        #all_sales_df = all_sales_df.join(product_dim_df, ["brand","model","color","Memory"],join_type='inner')
        all_sales_df = all_sales_df.join(product_dim_df, ["mobile_key"],join_type='inner')
        all_sales_df = all_sales_df.join(promo_code_dim_df, ["promotion_code","country", "region"],join_type='inner')
        all_sales_df = all_sales_df.join(region_dim_df, ["country", "region"],join_type='inner')
        all_sales_df = all_sales_df.selectExpr("sales_dwh.consumption.sales_fact_seq.nextval as ORDER_ID_PK", 
                                                "order_id as ORDER_CODE",                               
                                                "date_id_pk as DATE_ID_FK",          
                                                "region_id_pk as REGION_ID_FK",            
                                                "customer_id_pk as CUSTOMER_ID_FK",        
                                                "payment_id_pk as PAYMENT_ID_FK",         
                                                "product_id_pk as PRODUCT_ID_FK",          
                                                "promo_code_id_pk as PROMO_CODE_ID_FK",    
                                                "ORDER_QUANTITY",                          
                                                "LOCAL_TOTAL_ORDER_AMT",                   
                                                "LOCAL_TAX_AMT",                           
                                                "EXHCHANGE_RATE",                          
                                                "US_TOTAL_ORDER_AMT",                      
                                                "USD_TAX_AMT"                              
                                                )
        all_sales_df.write.save_as_table("sales_dwh.consumption.sales_fact",mode="append")


def main():
        session = get_snowpark_session()
        all_sales_query = """select * from curated.fr_sales_order
                            union
                            select * from curated.in_sales_order
                            union
                            select * from curated.us_sales_order
                        """
        sales_df = session.sql(all_sales_query)
        
        create_sales_fact(sales_df,session)  
        
        session.close()

if __name__ == '__main__':
    main()


