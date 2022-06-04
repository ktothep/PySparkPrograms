
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

session=SparkSession.builder.appName("Word Frequency").getOrCreate()
data_frame=session.read.option("inferSchema","true").option("header","true").format("csv").load("dataset/fakefriends-header.csv")
result=data_frame.groupBy("age").avg("friends").alias("friends_age").sort("age")
result.show(10)






