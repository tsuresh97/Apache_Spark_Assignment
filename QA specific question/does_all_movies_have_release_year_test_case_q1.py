####################################################################
# Code to check all the movie have a release year
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import re
sc = SparkContext("local", "Check all movies have release year")
sqlContext = SQLContext(sc)

movies_csv=sc.textFile("/home/suresh/Desktop/PySpark/ml-latest-small/movies.csv")
csv_header=(movies_csv.first())
remove_csv_header=movies_csv.filter(lambda line: line != csv_header)
cleanedRdd=remove_csv_header.map(lambda x:re.match( r'(.*".*)(,)(.*".*)', x, re.M|re.I).group(1)+re.match( r'(.*".*)(,)(.*".*)', x, re.M|re.I).group(3) if re.match( r'(.*".*)(,)(.*".*)', x, re.M|re.I) !=None else x)
splitting= cleanedRdd.map(lambda x:x.split(","))
row_data = splitting.map(lambda p: Row(re.findall(r'[0-9][0-9][0-9][0-9]+', p[1])))
dataFrame = sqlContext.createDataFrame(row_data,['Year'])

print("Number of movie have release year : %i" %(dataFrame.count()))

########################################################################
"""
                                                    *Test Case*
Pre-Processing:
Splitting the csv data into movieId, movie name and genre by comma.

Test Case 1:

Description: By checking the row contains 4 digit number.
Step 1: Have to count number of data which are present in the csv file by using count() function named as 'N1'.

Step 2: Have to search continuous 4 digit number by regular expression (i.e.) re.findall(r'[0-9][0-9][0-9][0-9]') if it satisfy increment the variable 'N2'.

Step 3: If N1 and N2 values are same, we can conclude all the films have release year.

Test Case 2:

Description: By checking the row contains 4 digit number within the bracket. In the dataset the year is enclosed with the bracket, even movie name might be a 4 digit number, so have to check is it present inside bracket.

Step 1: Have to count number of data which are present in the csv file by using count() function named as 'N1'.

Step 2: Have to search continuous 4 digit number by regular expression (i.e.) re.findall(r'[0-9][0-9][0-9][0-9]') inside the curve brackets if it satisfy increment the variable 'N2'.

Step 3: If N1 and N2 values are same, we can conclude all the films have release year.


Test Case 3:

Description: By checking the row contains 4 digit number within the bracket and it must be before comma. In the dataset the year is enclosed with the bracket, even movie name might be a 4 digit number with a bracket, so have to check is it present inside bracket and located at the last.

Step 1: Have to count number of data which are present in the csv file by using count() function named as 'N1'.

Step 2: Have to search continuous 4 digit number by regular expression (i.e.) re.findall(r'[0-9][0-9][0-9][0-9]') and it  must be after a character/word/words and also inside the curve brackets, if it satisfy increment the variable 'N2'.

Step 3: If N1 and N2 values are same, we can conclude all the films have release year.


"""