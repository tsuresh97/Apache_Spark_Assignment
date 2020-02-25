from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.sql.functions import count
from pyspark.sql.functions import col
sc = SparkContext("local", " Calculate most ratings, how many rates did he make?")
sqlContext = SQLContext(sc)

lines = sc.textFile("/home/suresh/Desktop/PySpark/ml-latest-small/ratings.csv")
h=(lines.first())
a=lines.filter(lambda line: line != h)
splitting = a.map(lambda max_value: max_value.split(","))
userId_rating = splitting.map(lambda p: Row(userId=int(p[0]),rating=float(p[2])))

schemaPeople_dataframe = sqlContext.createDataFrame(userId_rating)
schemaPeople_dataframe.groupBy("userId").agg(count("*"))
userId_count=schemaPeople_dataframe.groupBy("userId").agg(count("*"))
max_value=userId_count.select("count(1)").rdd.max()[0]

finalResult = userId_count.filter((col("count(1)") == max_value))
finalResult.show()


