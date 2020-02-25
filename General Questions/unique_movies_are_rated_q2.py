from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.sql.functions import col, countDistinct
sc = SparkContext("local", "Calculate how many unique movies are rated, how many are not rated")
sqlContext = SQLContext(sc)

movies_csv = sc.textFile("/home/suresh/Desktop/PySpark/ml-latest-small/movies.csv")
movies_csv_header=(movies_csv.first())
removing_header=movies_csv.filter(lambda line: line != movies_csv_header)
splitting = removing_header.map(lambda l: l.split(","))
movieId = splitting.map(lambda p: Row(movieId=int(p[0])))
movies_dataframe = sqlContext.createDataFrame(movieId)

ratings_csv = sc.textFile("/home/suresh/Desktop/PySpark/ml-latest-small/ratings.csv")
removing_rating_header=(ratings_csv.first())
removing_csv_header=ratings_csv.filter(lambda line: line != removing_rating_header)
splitting_rating = removing_csv_header.map(lambda l: l.split(","))
ratings_row = splitting_rating.map(lambda p1: Row(movieId=int(p1[1])))
ratings_dataframe = sqlContext.createDataFrame(ratings_row)

uniquely_rated_movie_count=ratings_dataframe.agg(countDistinct(col("movieId")).alias("movieId"))
uniquely_rated_movie_value=uniquely_rated_movie_count.select("movieId")
averageCount = (uniquely_rated_movie_value.groupBy().mean('movieId').collect())[0][0]

print("Total Number of films: %i"%(movies_dataframe.count()))
print("Films which are rated: %i"%(averageCount))
print("Films which are not rated: %i"%(movies_dataframe.count()-averageCount))






