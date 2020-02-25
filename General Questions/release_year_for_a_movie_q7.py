###############################################################################################
# Code to extraction of the release year
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


###############################################################################################
"""
                                                    *Steps to extract the release year*

Step 1:
Split the csv data into movieId, movie name and genre by comma.

Step 2:
In movie name, we have to search continuous 4 digit number by regular expression (i.e.) re.findall(r'[0-9][0-9][0-9][0-9]+', p[1]) and it must be after a character/word/words and also inside the curve brackets.

Step 3:
Create the dataframe from the extraction of the movie name(it contains the year alone).

Step 4:
Display the dataframe by using show() function.
"""