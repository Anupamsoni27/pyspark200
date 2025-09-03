import pyspark
from pyspark import SparkContext

sc = pyspark.SparkContext('local', 'test')

data = [1,2,3,4,5,6]

my_rdd = sc.parallelize(data)

new_rdd = my_rdd.map( lambda x: x *x)

print(new_rdd.collect())