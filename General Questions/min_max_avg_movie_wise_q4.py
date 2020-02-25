from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.sql.functions import avg
sc = SparkContext("local", " Calculate most ratings, how many rates did he make?")
sqlContext = SQLContext(sc)

ratings_csv = sc.textFile("/home/suresh/Desktop/PySpark/ml-latest-small/ratings.csv")
csv_header=(ratings_csv.first())
remove_csv_header=ratings_csv.filter(lambda line: line != csv_header)
splitting = remove_csv_header.map(lambda l: l.split(","))
row_data = splitting.map(lambda p: Row(movieId=int(p[1]),rating=float(p[2])))
dataFrame = sqlContext.createDataFrame(row_data,['MovieID','Value'])

dataFrame.groupBy("movieId").min("Value").show() #To display minimum rating for a movie
dataFrame.groupBy("movieId").max("Value").show()  #To display maximum rating for a movie
dataFrame.groupBy("movieId").avg("Value").show()  #To display average rating for a movie

