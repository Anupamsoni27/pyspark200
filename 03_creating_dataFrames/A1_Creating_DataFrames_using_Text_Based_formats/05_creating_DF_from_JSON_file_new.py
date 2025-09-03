from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Movie Data Analysis').getOrCreate()

moviesDF = spark.read.option('InferSchema','true').json('data/movies.json')

moviesDF.show()