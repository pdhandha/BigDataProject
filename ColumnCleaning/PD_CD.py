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
		return True
	elif re.match('[0-9]+',x):
		return True
	else :
		return False

def clean_invalid_data(header, data):
    filtered_data = data.filter(lambda x: (x == header) or (validity(x[8])))
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

    sc.stop()
