from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql import functions as func


spark=SparkSession.builder.appName("Movie Name").getOrCreate()

#get the superhero and their number of friends


lines=spark.read.format("text").load('dataset/Marvel/Marvel+Graph')
result_friends=lines.withColumn("id",func.split(func.col('value')," ")[0]) \
    .withColumn("Connections",func.size(func.split(func.col('value')," "))-1)\
    .groupBy("id").sum("Connections").alias("Connections").sort(col("sum(Connections)").asc())


schema_names=StructType([
    StructField("id",StringType()),
    StructField("name",StringType())
])

#Lookup function for Names.It returns a Map with id as key and name as value
def getName():
    nameMap={}
    with open('dataset/Marvel/Marvel+Names','r') as f:
        for lines in f:
            linee=lines.split()
            nameMap[linee[0]]=linee[1]
    return nameMap

#Creating broadcasr variable
getName_broadcast=spark.sparkContext.broadcast(getName())

#UDF to return Name from Id
def getNamefromId(id):
    return getName_broadcast.value[id]

#Register a UDF
udf_name=func.udf(getNamefromId)
fin_dataset=result_friends.withColumn("Name",udf_name(col("id")))




print(fin_dataset.show(10))