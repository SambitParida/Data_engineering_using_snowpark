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

def create_region_dim(all_sales_df, session) -> None:
        region_country_df = all_sales_df.groupBy(col('COUNTRY'),col('REGION')).count()
        region_dim_df = region_country_df.selectExpr("sales_dwh.consumption.region_dim_seq.nextval as rgion_id_pk","COUNTRY", "REGION")
        region_dim_df = region_dim_df.with_columns(["isActive","hash_key"],
                                                  [lit('Y'),hash(col('COUNTRY'),col('REGION'))])
        
        existing_region_df = session.sql("select hash_key from consumption.region_dim")
        
        region_dim_df = region_dim_df.join (existing_region_df,region_dim_df["hash_key"] == existing_region_df["hash_key"], join_type = 'leftanti')

        # distinct_values_df = region_dim_df.select(col("hash_key")).distinct()
        # distinct_list = [row[0] for row in distinct_values_df.collect()]
        
        insert_cnt = int(region_dim_df.count())
        target_table=session.table("consumption.region_dim")

        if insert_cnt>0:
                region_dim_df.write.save_as_table("sales_dwh.consumption.region_dim",mode="append")
                print("Data loaded to region dimension")
        else:
               print("No New Insert")

def create_product_dim(all_sales_df, session) -> None:
        product_dim_df = all_sales_df \
            .with_column("BRAND", split(col("MOBILE_KEY"), lit('/'))[0])  \
            .with_column("MODEL", split(col("MOBILE_KEY"), lit('/'))[1])  \
            .with_column("COLOR", split(col("MOBILE_KEY"), lit('/'))[2])  \
            .with_column("MEMORY", split(col("MOBILE_KEY"), lit('/'))[3]) \
            .with_column("HDD", split(col("MOBILE_KEY"), lit('/'))[4]) 
    
         
        product_dim_df = product_dim_df.groupBy(col('MOBILE_KEY'),col('BRAND'),col('MODEL'),col('COLOR'),col('MEMORY'),col('HDD')).count()
        
        product_dim_df = product_dim_df.selectExpr("sales_dwh.consumption.product_dim_seq.nextval as product_id_pk","MOBILE_KEY","BRAND", "MODEL","COLOR","MEMORY","HDD")
        product_dim_df = product_dim_df.with_columns(["isActive","hash_key"],
                                                  [lit('Y'),hash(col('BRAND'),col('MODEL'),col('COLOR'),col('MEMORY'),col('HDD'))])
       
        existing_product_dim_df = session.sql("select hash_key from consumption.product_dim")
        product_dim_df = product_dim_df.join (existing_product_dim_df,product_dim_df["hash_key"] == existing_product_dim_df["hash_key"], join_type = 'leftanti')

        product_dim_df.show(5)
        if int(product_dim_df.count())>0:
                product_dim_df.write.save_as_table("sales_dwh.consumption.product_dim",mode="append")
                print("data loaded to product dimension")
        else:
                print("No New Insert")

def create_promo_code_dim(all_sales_df, session) -> None:
        
        promo_code_df = all_sales_df.with_column("promotion_code", expr("case when promotion_code is null then 'NA' else promotion_code end"))
        promo_code_dim_df = promo_code_df.groupBy(col('promotion_code'),col('REGION'),col('COUNTRY')).count()                  
              
        promo_code_dim_df = promo_code_dim_df.selectExpr("sales_dwh.consumption.promo_code_dim_seq.nextval as promo_code_id_pk","PROMOTION_CODE","REGION","COUNTRY")
        promo_code_dim_df = promo_code_dim_df.with_columns(["isActive","hash_key"],
                                                   [lit('Y'),hash(col('promotion_code'),col('REGION'),col('COUNTRY'))])
       
        existing_promo_code_dim_df = session.sql("select hash_key from consumption.PROMO_CODE_DIM")
        promo_code_dim_df = promo_code_dim_df.join (existing_promo_code_dim_df,promo_code_dim_df["hash_key"] == existing_promo_code_dim_df["hash_key"], join_type = 'leftanti')

        promo_code_dim_df.show(5)
        if int(promo_code_dim_df.count())>0:
                 promo_code_dim_df.write.save_as_table("sales_dwh.consumption.promo_code_dim",mode="append")
                 print("data loaded to PROMO CODE dimension")
        else:
                 print("No New Insert")
      
def create_customer_dim(all_sales_df, session) -> None:
        
        CUSTOMER_dim_df = all_sales_df.groupBy(col("COUNTRY"),col("REGION"),col("CUSTOMER_NAME"),col("CONCTACT_NO"),col("SHIPPING_ADDRESS")).count()                  
  
        CUSTOMER_dim_df = CUSTOMER_dim_df.selectExpr("sales_dwh.consumption.CUSTOMER_dim_seq.nextval as CUSTOMER_id_pk","CUSTOMER_NAME",'CONCTACT_NO',"SHIPPING_ADDRESS","COUNTRY","REGION")
        CUSTOMER_dim_df = CUSTOMER_dim_df.with_columns(["isActive","hash_key"],
                                                   [lit('Y'),hash(col("COUNTRY"),col("REGION"),col("CUSTOMER_NAME"),col("CONCTACT_NO"),col("SHIPPING_ADDRESS"))])
       
        existing_CUSTOMER_dim_df = session.sql("select hash_key from consumption.CUSTOMER_DIM")
        CUSTOMER_dim_df = CUSTOMER_dim_df.join (existing_CUSTOMER_dim_df,CUSTOMER_dim_df["hash_key"] == existing_CUSTOMER_dim_df["hash_key"], join_type = 'leftanti')

        CUSTOMER_dim_df.show(5)
        if int(CUSTOMER_dim_df.count())>0:
                 CUSTOMER_dim_df.write.save_as_table("sales_dwh.consumption.CUSTOMER_dim",mode="append")
                 print("data loaded to customer dimension")
        else:
                 print("No New Insert")

def create_payment_dim(all_sales_df, session) -> None:
  
        PAYMENT_dim_df = all_sales_df.groupBy(col("PAYMENT_METHOD"),col("PAYMENT_PROVIDER"),col("COUNTRY"),col("REGION")).count()                  
  
        PAYMENT_dim_df = PAYMENT_dim_df.selectExpr("sales_dwh.consumption.PAYMENT_dim_seq.nextval as PAYMENT_id_pk","PAYMENT_METHOD",'PAYMENT_PROVIDER',"COUNTRY","REGION")
        PAYMENT_dim_df = PAYMENT_dim_df.with_columns(["isActive","hash_key"],
                                                   [lit('Y'),hash(col("PAYMENT_METHOD"),col("PAYMENT_PROVIDER"),col("COUNTRY"),col("REGION"))])
       
        existing_PAYMENT_dim_df = session.sql("select hash_key from consumption.PAYMENT_DIM")
        PAYMENT_dim_df = PAYMENT_dim_df.join (existing_PAYMENT_dim_df,PAYMENT_dim_df["hash_key"] == existing_PAYMENT_dim_df["hash_key"], join_type = 'leftanti')

        PAYMENT_dim_df.show(5)
        if int(PAYMENT_dim_df.count())>0:
                 PAYMENT_dim_df.write.save_as_table("sales_dwh.consumption.PAYMENT_dim",mode="append")
                 print("data loaded to PAYMENT dimension")
        else:
                 print("No New Insert")


def main():
        session = get_snowpark_session()
        all_sales_query = """select * from curated.fr_sales_order
                            union
                            select * from curated.in_sales_order
                            union
                            select * from curated.us_sales_order
                        """
        sales_df = session.sql(all_sales_query)
        
        create_region_dim(sales_df,session)  
        create_product_dim(sales_df,session)
        create_promo_code_dim(sales_df,session) 
        create_customer_dim(sales_df,session)
        create_payment_dim(sales_df,session)
                           

if __name__ == '__main__':
    main()


