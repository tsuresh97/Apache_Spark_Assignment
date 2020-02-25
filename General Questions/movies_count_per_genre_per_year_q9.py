from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import col, udf, count
from pyspark.sql.types import IntegerType
import re
sc = SparkContext("local", "Calculating the movies count per genre per year")
spark = SparkSession(sc)

def yearData(movieName):
    year = re.findall(r'.*([0-9][0-9][0-9][0-9])',movieName)
    if year:
        return int(year[0])
    else:
        return None
        
movies_csv = spark.read.csv("/home/suresh/Desktop/PySpark/ml-latest-small/movies.csv" , header=True)
allYearData = udf(lambda x: yearData(x),IntegerType())

movies_csv_year = movies_csv.withColumn('year',allYearData(col('title')))
movies_csv_genre_and_year = movies_csv_year.withColumn("genres", f.explode(f.split("genres","[|]")))
finalResult=movies_csv_genre_and_year.groupBy('year','genres').agg(count("*"))
finalResult.show()


