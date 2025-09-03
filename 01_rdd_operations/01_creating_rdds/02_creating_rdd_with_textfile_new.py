from pyspark import SparkContext

sc = SparkContext('local', "test")

rdd = sc.textFile("data/movies.csv")

print(rdd.collect())