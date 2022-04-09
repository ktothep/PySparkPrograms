from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark=SparkSession.builder.appName("First Program").getOrCreate()
dataset=spark.read.csv("dataset/Netflix.csv",header=True)
print(dataset.columns)
result=dataset.select("Titles").where(col("Netflix")==1).alias("Present on Netflix")
result2=dataset.select("Titles").where(col("Netflix")==1).agg(count("Titles")).alias("Count of shows")
print(result.show())
print(result2.show())




