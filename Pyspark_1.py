#Get the name of the shows present on Netflix
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

spark=SparkSession.builder.appName("First Program").getOrCreate()
dataset=spark.read.csv("dataset/Netflix.csv",header=True)
print(dataset.columns)
result=dataset.select("Titles","IMDB_Rating").where(col("Netflix")==1).alias("Present on Netflix").sort(col("IMDB_Rating").desc())
result2=dataset.select("Titles").where(col("Netflix")==1).agg(count("Titles")).alias("Count of shows")
print(result.show())
print(result2.show())




