from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.sql.functions import col, countDistinct
sc = SparkContext("local", "Calculating the users that have rated a movie but not tagged it")
sqlContext = SQLContext(sc)

ratings_csv = sc.textFile("/home/suresh/Desktop/PySpark/ml-latest-small/ratings.csv")
csv_header=(ratings_csv.first())
remove_csv_header=ratings_csv.filter(lambda line: line != csv_header)
splitting = remove_csv_header.map(lambda l: l.split(","))
rating_row_data = splitting.map(lambda p: Row(userId=p[0]))
rating_dataFrame = sqlContext.createDataFrame(rating_row_data)

tags_csv = sc.textFile("/home/suresh/Desktop/PySpark/ml-latest-small/tags.csv")
tags_csv_header=(tags_csv.first())
remove_tags_csv_header=tags_csv.filter(lambda line: line != csv_header)
tags_csv_splitting = remove_tags_csv_header.map(lambda l: l.split(","))
row_data_tags = tags_csv_splitting.map(lambda p: Row(userId=p[0]))
tags_dataFrame = sqlContext.createDataFrame(row_data_tags)

uniquely_rated_movie_count=tags_dataFrame.agg(countDistinct(col("userId")).alias("userId"))
uniquely_rated_movie_value=uniquely_rated_movie_count.select("userId")
averageCount = (uniquely_rated_movie_value.groupBy().mean('userId').collect())[0][0]

uniquely_rated_movie_count1=rating_dataFrame.agg(countDistinct(col("userId")).alias("userId"))
uniquely_rated_movie_value1=uniquely_rated_movie_count1.select("userId")
averageCount1 = (uniquely_rated_movie_value1.groupBy().mean('userId').collect())[0][0]

print("%i users that have rated a movie but not tagged it "%(averageCount1-averageCount))
