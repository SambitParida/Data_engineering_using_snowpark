## Project Name
Create a data pipeline to support analytics of Amazon orders information from India, US & France

## Project Description
The implementation involves below key activities
1. Ingest related data (customer, restaurant, location, address, menu and order etc.) from various sources as internal stages.
2. Identify dimensions and facts. Design data pipeline involving bronze, silver and gold layers following medallion architecture 
3. Curate source data and merge to create a snapshot view by implementing SCD1.
4. Identify changes in dimension tables and maintain history in gold (consumption) layer.
5. Create business views and dynamic tables to support reporting analytics.
6. Perform tag based column level data masking to obfuscate PII data.

## Data Flow Diagram

![Alt text](https://github.com/SambitParida/swiggy_data_pipeline/blob/dev/pics/DataFlow_Swiggy.png)


## Snowflake Concepts used

1. Internal stage
2. Implement Slowly Changing Dimensions II using merge
3. Views and Dynamic Tables
4. Column level masking of PII data
5. Create and maintain fact and dimension tables.
6. Streams

Tools Used: 
1. Snowflake Business Critical Edition
2. VSCODE
