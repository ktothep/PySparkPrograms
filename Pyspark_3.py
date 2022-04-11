#UDF Example
from pyparsing import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import  FloatType

def muli(x):
    return x*100
multi=udf(lambda x:muli(x))
spark=SparkSession.builder.appName("First Program").getOrCreate()
spark.udf.register("multi",multi)
dataset=spark.read.csv("dataset/Netflix.csv",header=True)
dataste2=dataset.withColumn("Ample",multi("IMDB_Rating"))
print(dataste2.show(6))

result=dataset.select("Titles","IMDB_Rating").where(col("Netflix")==0)


print(result.show())


