from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
sc = SparkContext("local", "Rated AND tagged the movie")
sqlContext = SQLContext(sc)

ratings_csv = sc.textFile("/home/suresh/Desktop/PySpark/ml-latest-small/ratings.csv")
rating_header=(ratings_csv.first())
removing_rating_header=ratings_csv.filter(lambda line: line != rating_header)
splitting_rating = removing_rating_header.map(lambda l: l.split(","))
rating_userId = splitting_rating.map(lambda p: Row(userId=p[0]))
rating_dataframe = sqlContext.createDataFrame(rating_userId)

tags_csv = sc.textFile("/home/suresh/Desktop/PySpark/ml-latest-small/tags.csv")
splitting_tags = tags_csv.map(lambda l: l.split(","))
tags_userId = splitting_tags.map(lambda p: Row(userId=p[0]))
tags_dataframe = sqlContext.createDataFrame(tags_userId)

newSalesHire = rating_dataframe.select('userId').intersect(tags_dataframe.select('userId'))
print("The number of users rated and tagged the movie are %i"%(newSalesHire.count()))
