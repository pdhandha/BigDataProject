from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import re

def check_datatype(x):
    if x is "" or x is " ":
        return "NULL"
    elif re.match('[0-9]+', x):
        return type(int.__class__)
    else:
        return type(x)

def validity(x):
	if x is "" or x is " ":
		return False
	elif re.match('[0-9]+',x):
		return True
	else :
		return False

def clean_invalid_data(header, data):
    filtered_data = data.filter(lambda x: (x == header) or (validity(x[6])))
    return filtered_data

def toCSVLine(data):
    return ','.join(data)

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    header = lines.first()  # extract header

    lines = clean_invalid_data(header, lines)
    lines = lines.map(toCSVLine)
    lines.saveAsTextFile("crime_clean.out")
    # header = lines.first()
    # lines = lines.filter(lambda x: x != header)
    #
    # columnData = lines.map(lambda x: (x[0], x[6], check_datatype(x[6]), validity(x[6])))
    # #columnData = columnData.filter(lambda x: x[3] == "VALID") #This line is used to filter the data and remove all invalid entries
    # columnData.saveAsTextFile("col7.out")
    #
    # lines = lines.map(lambda x: (validity(x[6]), 1)).reduceByKey(lambda x, y: x + y).collect()
    # lines = sc.parallelize(lines)
    # lines.saveAsTextFile("col7Stats.out")

    sc.stop()
