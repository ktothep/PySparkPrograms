#Movie Rating by Stars
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

session=SparkSession.builder.appName("RatingGroup").getOrCreate()
scheam_name=StructType([
    StructField("id",StringType()),
    StructField("item", StringType()),
    StructField("rating", StringType()),
    StructField("timestamp", StringType())
])
all_rating=session.read.format("csv").schema(scheam_name).load("dataset/rating.csv")
all_rating_count=all_rating.groupBy(col("rating")).count().sort(col("rating"))
print(all_rating_count.show())


