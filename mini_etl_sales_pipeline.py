import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)

# urls_and_paths = {
#     "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt":os.path.join(data_dir, "test.txt"),
#     "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe":os.path.join(hadoop_home, "bin", "winutils.exe"),
#     "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll":os.path.join(hadoop_home, "bin", "hadoop.dll")
# }
#
# # Create an unverified SSL context
# ssl_context = ssl._create_unverified_context()
#
# for url, path in urls_and_paths.items():
#     # Use the unverified context with urlopen
#     with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
#         data = response.read()
#         out_file.write(data)
# import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['JAVA_HOME'] = r'C:\Users\DELL\.jdks\corretto-1.8.0_442'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

sales_data = [
    (1, "IT", "2024-01", 1000),
    (2, "IT", "2024-02", 1500),
    (3, "IT", "2024-03", 1200),
    (4, "HR", "2024-01", 800),
    (5, "HR", "2024-02", 900),
    (6, "HR", "2024-03", None),
    (7, "Finance", "2024-01", 2000),
    (8, "Finance", "2024-02", 2200),
    (8, "Finance", "2024-02", 2200)  # duplicate
]

columns = ["id", "department", "month", "sales"]

df =spark.createDataFrame(sales_data,schema=columns)
df.show()

# 1️⃣ Remove null sales
# 2️⃣ Remove duplicates
# 3️⃣ Calculate total sales per department
# 4️⃣ Calculate month-over-month growth using lag()
# 5️⃣ Write result to Parquet (partitionBy department)

# 1️⃣ Remove null sales
df1 = df.dropna(subset=["sales"])
df1.show()

# 2️⃣ Remove duplicates
df2 = df1.dropDuplicates()
df2.show()

# 3️⃣ Calculate total sales per department
df3 = df2.groupBy("department").agg(sum("sales").alias("Toatal_sales"))
df3.show()

from pyspark.sql.window import *

# 4️⃣ Calculate month-over-month growth using lag()

window_df = Window.partitionBy("department").orderBy("month")
df4 = df2.withColumn("previous_month_sales",lag("sales",1).over(window_df))
df4.show()

df5 = df4.withColumn("month-over-month",expr("sales-previous_month_sales"))
df5.show()


