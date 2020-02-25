from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import  IntegerType
import re
import datetime
sc = SparkContext("local", "Tests to ensure that the rating & tag happened on or after the year that the movie was released")
sqlContext = SQLContext(sc)

movies_csv=sc.textFile("/home/suresh/Desktop/PySpark/ml-latest-small/movies.csv")
movies_csv_header=(movies_csv.first())
remove_movies_csv_header=movies_csv.filter(lambda line: line != movies_csv_header)

cleanedRdd=remove_movies_csv_header.map(lambda x:re.match( r'(.*".*)(,)(.*".*)', x, re.M|re.I).group(1)+re.match( r'(.*".*)(,)(.*".*)', x, re.M|re.I).group(3) if re.match( r'(.*".*)(,)(.*".*)', x, re.M|re.I) !=None else x)
splitting_movies= cleanedRdd.map(lambda x:x.split(","))
movies_row = splitting_movies.map(lambda p: Row(Year=int(p[0]),MovieId=(re.findall(r'([0-9][0-9][0-9][0-9])', p[1]))))
movies_dataFrame = sqlContext.createDataFrame(movies_row,['Year','MovieId'])
movies_dataFrame = movies_dataFrame.withColumn("MovieId", movies_dataFrame["MovieId"].cast(IntegerType()))

ratings_csv=sc.textFile("/home/suresh/Desktop/PySpark/ml-latest-small/ratings.csv")
ratings_csv_header=(ratings_csv.first())
remove_ratings_csv_header=ratings_csv.filter(lambda line: line != ratings_csv_header)
ratings_csv_splitting= remove_ratings_csv_header.map(lambda x:x.split(","))
ratings_row = ratings_csv_splitting.map(lambda p: Row(MovieId=int(p[1]),TimeStamp=str(datetime.datetime.fromtimestamp(int(p[3])).strftime('%Y'))))
ratings_dataFrame = sqlContext.createDataFrame(ratings_row,['MovieId','TimeStamp'])

tags_csv=sc.textFile("/home/suresh/Desktop/PySpark/ml-latest-small/tags.csv")
tags_csv_header=(tags_csv.first())
remove_tags_csv_header=tags_csv.filter(lambda line: line != tags_csv_header)
tags_splitting= remove_tags_csv_header.map(lambda x:x.split(","))
tags_row = tags_splitting.map(lambda p: Row(MovieId=int(p[1]),TimeStamp=int(datetime.datetime.fromtimestamp(int(p[3])).strftime('%Y'))))
tags_dataFrame = sqlContext.createDataFrame(tags_row,['MovieId','TimeStamp'])

check_movie_with_rating = movies_dataFrame.join(ratings_dataFrame, (movies_dataFrame.Year[0] > ratings_dataFrame.TimeStamp) & (movies_dataFrame.Year[1] > ratings_dataFrame.TimeStamp) & (movies_dataFrame.MovieId == ratings_dataFrame.MovieId))

check_movie_with_tags = movies_dataFrame.join(tags_dataFrame, (movies_dataFrame.Year[0] > tags_dataFrame.TimeStamp) & (movies_dataFrame.Year[1] > tags_dataFrame.TimeStamp) & (movies_dataFrame.MovieId == tags_dataFrame.MovieId))

print("Total Number of flim : %i" %(movies_dataFrame.count()))
print("Total number of flim rated before the year that the movie was released  : %i" %(check_movie_with_rating.count()))
print("Total number of flim tagged before the year that the movie was released : %i" %(check_movie_with_tags.count()))

