# pyspark-mini-etl-project
Mini ETL Sales Pipeline using PySpark
# Mini ETL Sales Pipeline using PySpark

## 📌 Project Overview
This project demonstrates a simple ETL pipeline built using PySpark.

## 🔄 ETL Process
1. Extract: Loaded sales data into Spark DataFrame  
2. Transform:
   - Removed null values  
   - Removed duplicate records  
   - Calculated department-wise total sales  
   - Computed month-over-month sales growth using window functions  
3. Load:
   - Stored cleaned data in Parquet format partitioned by department  

## 🛠 Technologies Used
- Python
- PySpark
- Window Functions
- Parquet

## 📊 Concepts Covered
- dropna()
- dropDuplicates()
- groupBy()
- lag()
- partitionBy()
- DataFrame API
