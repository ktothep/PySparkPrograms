from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql import functions as func


spark=SparkSession.builder.appName("Movie Name").getOrCreate()

def loadName():
    movieDict={}
    with open('dataset/ml-100k/u.item','r') as f:
        for lines in f:
            line=lines.split("|")
            movieDict[line[0]]=line[1]
    return movieDict

dict_movie=spark.sparkContext.broadcast(loadName())
scheama_name=StructType([
    StructField("id",StringType()),
    StructField("item", StringType()),
    StructField("rating", StringType()),
    StructField("timestamp", StringType())])

movie_dataframe=spark.read.format('csv').schema(scheama_name).load('dataset/ml-100k/rating.csv')
result_agg=movie_dataframe.groupBy("id").count()

def giveTitle(id):
    return dict_movie.value[id]

lookup=func.udf(giveTitle)
result_new_column=result_agg.withColumn("MovieTitle",lookup("id"))

result_new_column.show(10)
