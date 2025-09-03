from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('Read Nested JSON') \
    .getOrCreate()

df = spark.read.option('multiline', 'true').json('data/nested_json.json')

df.printSchema()

df.show()