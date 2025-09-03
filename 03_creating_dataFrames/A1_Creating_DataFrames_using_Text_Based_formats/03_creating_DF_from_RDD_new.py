from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext("local", "TextFileExample")
my_rdd = sc.textFile('data/employee.txt')
print(my_rdd.collect())

header = my_rdd.first()
data_rdd = my_rdd.filter(lambda x : x != header)

data_rdd2 = data_rdd.map(lambda x : x.split(','))

data_rdd3 = data_rdd2.map(lambda x: (int(x[0]), x[1], x[2], float(x[3]), x[4]))

spark = SparkSession.builder.appName('Creating DataFrame from RDD').getOrCreate()

empDF = spark.createDataFrame(data_rdd2, schema = ['eid', 'ename', 'dept', 'salary', 'date_of_joining'])
empDF2 = spark.createDataFrame(data_rdd3, schema = ['eid', 'ename', 'dept', 'salary', 'date_of_joining'])

empDF.show()
empDF2.show()
