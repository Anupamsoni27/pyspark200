from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Movie Data Analysis')\
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0")\
    .getOrCreate()

# Reading data from an XML file into a DataFrame
df = spark.read.format('xml').option('rowTag', 'person')\
    .load(r"/Users/anupamsoni/PycharmProjects/pyspark200_new/03_creating_dataFrames/A1_Creating_DataFrames_using_Text_Based_formats/data/persons.xml")

df.printSchema()
df.show(truncate=False)