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


def traverse_directory(directory, file_extension) -> list:
    local_file_path = []
    file_name = []
    partition_dir = []

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(file_extension):
                file_path = os.path.join(root,file)
                file_name.append(file)
                partition_dir.append(root.replace(directory,""))
                local_file_path.append(file_path)
    return file_name, partition_dir, local_file_path


def main():
    load_dotenv()
    directory_path=os.getenv("directory_path")
    csv_file_name, csv_partition_dir , csv_local_file_path= traverse_directory(directory_path,'.csv')
    parquet_file_name, parquet_partition_dir , parquet_local_file_path= traverse_directory(directory_path,'.parquet')
    json_file_name, json_partition_dir , json_local_file_path= traverse_directory(directory_path,'.json')
    stage_location = '@sales_dwh.source.my_internal_stg'

    csv_index = 0
    for file_element in csv_file_name:
        put_result = (
                    get_snowpark_session().file.put(
                         csv_local_file_path[csv_index],
                         stage_location+"/"+csv_partition_dir[csv_index],
                         auto_compress=False, overwrite=True, parallel = 10 
                    )
        )
        print ("{} file has been loaded to internal stage".format(file_element))
        csv_index = csv_index+1

    parquet_index = 0
    for file_element in parquet_file_name:
        put_result = (
                    get_snowpark_session().file.put(
                         parquet_local_file_path[parquet_index],
                         stage_location+"/"+parquet_partition_dir[parquet_index],
                         auto_compress=False, overwrite=True, parallel = 10 
                    )
        )
        print ("{} file has been loaded to internal stage".format(file_element))
        parquet_index = parquet_index+1

    json_index = 0
    for file_element in json_file_name:
        put_result = (
                    get_snowpark_session().file.put(
                         json_local_file_path[json_index],
                         stage_location+"/"+json_partition_dir[json_index],
                         auto_compress=False, overwrite=True, parallel = 10 
                    )
        )
        print ("{} file has been loaded to internal stage".format(file_element))
        json_index = json_index+1

    
if __name__ == '__main__':
    main()


