from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, spark_partition_id, when, col, struct, array
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, DoubleType, StringType

spark = SparkSession.builder.appName('SparkApp').getOrCreate()

data1 = [
    [1, "abc", "DE", 5.8, "2017-02-03"],
    [2, "xyz", "DEV", 6.7, "2019-03-09"],
    [3, "pqr", "DE", 10.5, "2014-05-15"]
]

data2 = [
    [1, "abc", "DE", 5.8, "2017-02-03"],
    [2, "xyz", "DEV", 6.7, "2019-03-09"],
    [3, "pqr", "DE", 10.5, "2014-05-15"]
]

data3 = [
    (1, ("abc", "DE", "5.8"), "2017-02-03"),
    (2, ("xyz", "DEV", "6.7"), "2019-03-09"),
    (3, ("pqr", "DE", "10.5"), "2014-05-15"),
    (4, ("lmn", "HR", "5.9"), "2018-04-17"),
    (5, ("qwr", "HR", "6.5"), "2020-06-26"),
    (6, ("rst", "DEV", "4.6"), "2016-04-27")
]
# with default schema
columns1 = ['eid', 'ename', 'dept', 'salary', 'date_of_joining']
columns3 = ['eid', 'personal_details', 'date_of_joining']


employee_df = spark.createDataFrame(data1, columns1)
employee_df.printSchema()

# with explicit schema

schema1 = StructType([
    StructField('eid', IntegerType(),False),
    StructField('ename', StringType(), True),
    StructField('dept', StringType(), True),
    StructField('salary', DoubleType(), True),
    StructField('date_of_joining', StringType(), True),

])
df2 = spark.createDataFrame(data1, schema1)
df3 = spark.createDataFrame(data2, schema1)
# 1. Combine two DFs and remove duplicates
df_combined = df2.union(df3)
df_combined.show()

schema2 = StructType([StructField('eid', IntegerType(),True),
                      StructField('personal_details',
                                  StructType([StructField('ename', StringType(), True),
                                              StructField('dept', StringType(), True),
                                              StructField('salary', StringType(), True)])
                                  ),
                      StructField('date_of_joining', StringType(), True)])

df4 = spark.createDataFrame(data3, schema2)
df4.show()

# 2. Create a DF with range 100, and take it's samples :
#   i. take 10% sample with replacement
#  ii. take 10% sample withoutReplacement
#  iii. take 10% sample using seed

df5 = spark.range(100).withColumnRenamed('id', 'numbers')
# df4 = spark.createDataFrame(data4, StructType([StructField('numbers', IntegerType(), True)]))
# df5_sample = df5.sample(withReplacement=False, fraction=0.1, seed=42)
# df5_sample.show()
print(df5.rdd.getNumPartitions())
# 3. Write expression to add a new column to DF with a unique 64-bit integer ID for all rows.
df_with_id = df5.withColumn('_id', monotonically_increasing_id() )
df_with_id.show()
df_with_id.withColumn("part_id", spark_partition_id()).show(100)
# 4. Fill NULL values in DF crDF with some values
df_with_id_with_na = df_with_id.withColumn('numbers_check', when(col('numbers') % 10 == 0, None).otherwise(col('numbers')))
df_with_id_with_na.show()
# 5. We have a DF which is as follows,
# +---+-----+----+------+---------------+
# |eid|ename|dept|salary|date_of_joining|
# +---+-----+----+------+---------------+
# |  1|  abc|  DE|   5.8|     2017-02-03|
# |  2|  xyz| DEV|   6.7|     2019-03-09|
# |  3|  pqr|  DE|  10.5|     2014-05-15|
# |  4|  lmn|  HR|   5.9|     2018-04-17|
# |  5|  qwr|  HR|   6.5|     2020-06-26|
# |  6|  rst| DEV|   4.6|     2016-04-27|
# +---+-----+----+------+---------------+
# perform transformations so that, it should appear as follows,
# +---+----------------+---------------+
# |eid|personal_details|date_of_joining|
# +---+----------------+---------------+
# |  1|  [abc, DE, 5.8]|     2017-02-03|
# |  2| [xyz, DEV, 6.7]|     2019-03-09|
# |  3| [pqr, DE, 10.5]|     2014-05-15|
# |  4|  [lmn, HR, 5.9]|     2018-04-17|
# |  5|  [qwr, HR, 6.5]|     2020-06-26|
# |  6| [rst, DEV, 4.6]|     2016-04-27|
# +---+----------------+---------------+
new_employee_df = employee_df.select(col('eid'), array(col('ename'), col('dept'), col('salary')).alias('personal_details'), col('date_of_joining'))

# 6. Select 0th index in personal_details column from employee DF
new_employee_df.select(col('eid'), col('personal_details')[0].alias('name'), col('date_of_joining')).show()


