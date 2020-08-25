from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import col
import re
sc = SparkContext("local", "Calculating the movies with the 'Drama' genre")
sqlContext = SQLContext(sc)

movies_csv=sc.textFile("/home/suresh/Desktop/PySpark/ml-latest-small/movies.csv")
csv_header=(movies_csv.first())
remove_csv_header=movies_csv.filter(lambda line: line != csv_header)

words_filter = remove_csv_header.filter(lambda x: 'Drama' in x)
filtered = words_filter.collect()

print("There are %i movies with the 'Drama' genre " %(words_filter.count()))

# Added comments
