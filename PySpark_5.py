#Average Number of friends by Age
from pyspark import SparkContext

sc=SparkContext()

friends_data=sc.textFile("dataset/fakefriends.csv")

def getPairs(lines):
    line = lines.split(",")
    age,friends=line[2],line[3]
    return (age,friends)
rdd=friends_data.map(getPairs);
total=rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
averageByAge=total.mapValues(lambda x:x[0]/x[1])
result=averageByAge.collect()
for r in result:
    print(r)




