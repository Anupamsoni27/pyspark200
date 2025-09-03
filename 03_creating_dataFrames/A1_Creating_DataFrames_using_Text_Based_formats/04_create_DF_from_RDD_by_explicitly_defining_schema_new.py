from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

sc = SparkContext("local", "TextFileExample")
my_rdd = sc.textFile('/Users/anupamsoni/PycharmProjects/pyspark200_new/03_creating_dataFrames/A1_Creating_DataFrames_using_Text_Based_formats/data/employee.txt')

header = my_rdd.first()
my_rdd = my_rdd.filter(lambda x : x != header)

my_rdd = my_rdd.map(lambda x : x.split(','))
my_rdd = my_rdd.map(lambda x : (int(x[0]), x[1], x[2], float(x[3]), x[4]))

schema = StructType([StructField('id', IntegerType(), True),
                     StructField('name', StringType(), True),
                     StructField('dept', StringType(), True),
                     StructField('salary', DoubleType(), True),
                     StructField('doj', StringType(), True)
                    ])

spark = SparkSession.builder.appName('TextFileExample').getOrCreate()

empDF = spark.createDataFrame(my_rdd, schema=schema)

empDF.show()

empDF.printSchema()
