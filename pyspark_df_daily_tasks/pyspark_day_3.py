from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp

sc = SparkSession.builder.appName("Aatank").getOrCreate()

employeeDF = sc.read.csv("../04_dataframe_ops_basics/02_withColumn_and_withColumnRenamed/data/employee.csv", header=True)

# For a DF with a date column "event_date", change it to "event_date_time" with timestamp datatype
employeeDF.withColumn('event_date_time', to_timestamp(col('date_of_joining'))).printSchema()

 # Count distinct "cid" from crDF.
print(employeeDF.select('eid').distinct().count())

 # Order results by descending revenue column, null values should appear first.


 # Replace all NULL values in revenue column with 50


