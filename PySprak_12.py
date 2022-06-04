from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType,StringType

session=SparkSession.builder.appName("Movie Reccomendation").getOrCreate()


def loadMovies():
    movienames = {}
    with open("dataset/ml-100k/u.item","r") as data:
        for lines in data:
            values=data.read().split("|")
            movienames[int(values[0])]=values[1]
    return movienames

movie_Schema=StructType([
    StructField("userID",IntegerType()),
    StructField("rating", IntegerType()),
    StructField("movie", IntegerType()),
    StructField("timestamp", LongType())
])
names=loadMovies()
ratings=session.read.format("csv").option("sep","\t").load("dataset/ml-100k/u.data")

als=ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userID").setItemCol("movie").setRatingCol("rating")
model=als.fit(ratings)






