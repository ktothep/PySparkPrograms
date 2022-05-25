from pyspark import SparkContext

sc=SparkContext()

temp_data=sc.textFile("dataset/1800.csv")

def parse(lines):
    line = lines.split(",")
    stationid, entrytype, temprature = line[0], line[2], line[3]
    return (stationid,entrytype,temprature)

rdd=temp_data.map(parse)
filtered_rdd=rdd.filter(lambda x:x[1]=='TMIN')
station_map=filtered_rdd.map(lambda x:(x[0],x[2]))
station_map.reduceByKey(lambda x,y:min(x,y))