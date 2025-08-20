from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, dayofweek, date_sub

sc = SparkSession.builder.appName("Aatank").getOrCreate()

employeeDF = sc.read.csv("../04_dataframe_ops_basics/02_withColumn_and_withColumnRenamed/data/employee.csv", header=True)
new_employeeDF = employeeDF.withColumn('salary', col('salary').cast('int'))
new_employeeDF.printSchema()
# Select ename and increment , increment by 20% if salary is less than 6.0 and increment is 0 for salary greater than 6.0
# new_employeeDF.select('ename', 'salary').show()
employeeDF_incremented = new_employeeDF.withColumn('increment', (when(col('salary') < 60000, col('salary')* 1.20 ).otherwise(col('salary')).cast('int')))
employeeDF_incremented.show(10)

# Rename "ename" column from employeeDF to "employee_name".
employeeDF_incremented_new = employeeDF_incremented.withColumnRenamed('ename','employee_name')
employeeDF_incremented_new.show(5)

# Change datatype of string column to double
employeeDF_incremented_new.select(col('eid'), col('employee_name'), col('dept'), col('salary'), col('date_of_joining'), col('increment').cast('double')).printSchema()

# Create a DF with date datatype column and display date a week before the current_date
employeeDF_incremented_new.withColumn('week_ago', date_sub(to_date(col('date_of_joining')), 7)).show(5)



