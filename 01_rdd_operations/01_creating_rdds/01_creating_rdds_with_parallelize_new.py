from pyspark import SparkContext

sc = SparkContext('local', 'Appname')

data = ['1', '2', '3', '4', '5']

my_rdd = sc.parallelize(data, numSlices=4)

print("Number of partitions:", my_rdd.getNumPartitions())

print((my_rdd.glom().collect()))