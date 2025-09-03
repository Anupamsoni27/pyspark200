# Create a DatFrame from employee.txt (csv) by providing schema explicitly

# Read Modes :
# PERMISSIVE(Default): Whenever schema mismatch happens : insert NULL for mismatched values
# FAILFAST : Whenever schema mismatch happens : ERROR will occur
# DROPMALFORMED : Whenever schema mismatch happens : DROP mismatched Records

from pyspark.sql import SparkSession

class ReadMode:
    PERMISSIVE = "PERMISSIVE"
    FAILFAST = "FAILFAST"
    DROPMALFORMED = "DROPMALFORMED"

spark = SparkSession.builder.appName('Create_dataframe').getOrCreate()

df = spark.read.option('mode', ReadMode.FAILFAST).csv('data/employee.txt', header=True)

print(df.show())