from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

sc = SparkSession.builder.appName("Aatank").getOrCreate()

employeeDF = sc.read.csv("../04_dataframe_ops_basics/02_withColumn_and_withColumnRenamed/data/employee.csv", header=True)

# Display all records of employeeDF
# employeeDF.show()

# Display first three records on employeeDF
employeeDF.show(3)

# PrintSchema for employeeDF
print(employeeDF.printSchema())

employeeDF.select(col('eid'), col('ename')).show(2)


 # Add new column "salary_increment" by Incrementing the salary by 15% for employeeDF
employeeDF_with_increment = employeeDF.withColumn("salary_increment", (col('salary').cast('int') * 1.15).cast('int'))
employeeDF_with_increment.show(4)

 # Add new column "salary_increment" by Incrementing the salary by 15% if salary is greater than 6.0 and Increment by 18% if salary is less thamn 6.0
employeeDF_with_increment_conditional = employeeDF.withColumn("salary_increment", when( col('salary').cast('int') > 60000 ,col('salary').cast('int') * 1.15).otherwise(col('salary').cast('int') * 1.15).cast('int'))
employeeDF_with_increment_conditional.show(6)


 # Add new column "salary_increment" by incrementing the salary by
	# i. 30 % if Dept is DE,DEV and salary < 6.0
	#    25% if Dept is DE,DEV and salary > 6.0
	# ii.20% if Dept is HR and salary < 6.0
	#    18% if Dept is HR and salary > 6.0

employeeDF_with_increment_conditional_2 = employeeDF.withColumn('salary_increment',
                                                                when(col('dept').isin("DE","DEV") & (col('salary').cast('int') < 60000), '30')
                                                                .when(col('dept').isin("DE","DEV") & (col('salary').cast('int') > 60000),'25')
                                                                .when(col('dept').isin("HR") & (col('salary').cast('int') < 60000),'20')
                                                                .when(col('dept').isin("HR") & (col('salary').cast('int') > 60000),'15')
                                                                .otherwise(col('salary')))
employeeDF_with_increment_conditional_2.filter("dept ='HR'").show(10)
# employeeDF_with_increment.show(5)

input("Press Enter to End")

