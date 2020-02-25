from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import re
sc = SparkContext("local", "Extraction of the release year")
sqlContext = SQLContext(sc)

movies_csv=sc.textFile("/home/suresh/Desktop/PySpark/ml-latest-small/movies.csv")
csv_header=(movies_csv.first())
remove_csv_header=movies_csv.filter(lambda line: line != csv_header)
cleanedRdd=remove_csv_header.map(lambda x:re.match( r'(.*".*)(,)(.*".*)', x, re.M|re.I).group(1)+re.match( r'(.*".*)(,)(.*".*)', x, re.M|re.I).group(3) if re.match( r'(.*".*)(,)(.*".*)', x, re.M|re.I) !=None else x)
splitting= cleanedRdd.map(lambda x:x.split(","))
row_data = splitting.map(lambda p: Row(p[1],re.findall(r'[0-9][0-9][0-9][0-9]+', p[1])))
dataFrame = sqlContext.createDataFrame(row_data,['Flim','Year'])
dataFrame.show()
print("Total Number of flim : %i" %(dataFrame.count()))

