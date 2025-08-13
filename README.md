## Project Name
Create a data pipeline to consume Amazon orders information from India, US & France and support analytics.

## Project Description
The implementation involves below key activities
1. Identify and define dimensions and facts from amazon sales order data collected from US, france and India.
2. Sales order data from different countries have been created in different formats.
US - parquet, France - JSON & India - CSV
3. Create snowflake internal stages and file formats.
4. Identify dimensions and facts. Design data pipeline involving bronze, silver and gold layers following medallion architecture 
5. Curate source data and merge to create a snapshot view by implementing SCD1.
6. Final consumption layer contains dimensions and facts created out of sales order information.

## Snowflake Concepts used
1. Internal stage
2. Snowpark Python API
3. Snowflake Sequences
4. Use Hash keys for maintaining SCD
5. Create and maintain fact and dimension tables.

Tools Used: 
1. Snowflake Business Critical Edition
2. VSCODE


## References  
https://www.youtube.com/watch?v=1jC98XQwBZw  
Data Engineering Simplified