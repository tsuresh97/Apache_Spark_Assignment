from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.sql.functions import col
sc = SparkContext("local", "Tests to ensure that at least 50% of movies have more than one genres")
sqlContext = SQLContext(sc)

movies_csv = sc.textFile("/home/suresh/Desktop/PySpark/ml-latest-small/movies.csv")
splitting = movies_csv.map(lambda l: l.split("|")) #Split by '|' symbol
genres = splitting.map(lambda p: Row(len(p)))
genresDataFrame = sqlContext.createDataFrame(genres,['Genres'])
finalResult = genresDataFrame.filter((col("Genres") >1))

print("Total Movie: %s"%(genresDataFrame.count()))
print("More than one genres Movie: %s"%(finalResult.count()))

if((genresDataFrame.count()/2)<finalResult.count()):
    print("50% of movies having more than one genres")
else:
    print("50% of movies not having more than one genres")

