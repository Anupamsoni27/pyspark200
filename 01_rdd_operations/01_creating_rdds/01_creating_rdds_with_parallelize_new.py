from pyspark import SparkContext

sc = SparkContext("local"," Test" )

data = [1,2,3,4,5]
rdd = sc.parallelize(data)
print(rdd.collect())
input("Press Enter to Exit : ")

#print(sc)