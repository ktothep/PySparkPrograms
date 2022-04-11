#Get the name of the average rating of shows present on Netflix and not on Netflix
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

spark=SparkSession.builder.appName("First Program").getOrCreate()
dataset=spark.read.csv("dataset/Netflix.csv",header=True)
print(dataset.columns)
avg_on_netflix=dataset.select("IMDB_Rating").where(col("Netflix")==1).agg(avg('IMDB_Rating'))
avg_not_on_netflix=dataset.select("IMDB_Rating").where(col("Netflix")==0).agg(avg('IMDB_Rating'))

print(avg_on_netflix.show())

print(avg_not_on_netflix.show())






