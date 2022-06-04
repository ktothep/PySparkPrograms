
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

session=SparkSession.builder.appName("Word Frequency").getOrCreate()
schema_name=StructType([
    StructField("customer_id",StringType()),
    StructField("item_id", StringType()),
    StructField("amount", FloatType())])
dataframe_lines=session.read.format("csv").schema(schema_name).load("dataset/customer-orders.csv")
result=dataframe_lines.groupBy('customer_id').sum("amount").withColumnRenamed("sum(amount)","total").sort(col("total").desc())

print(result.show())





