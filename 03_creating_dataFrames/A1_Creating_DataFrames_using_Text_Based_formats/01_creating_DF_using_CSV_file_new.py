# Create a DataFrame from employee.txt (csv) file by implicitly providing Schema

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Createing_datafroame").getOrCreate()

df = spark.read.csv("data/employee.txt", inferSchema=True, header=True)

df.show(5)