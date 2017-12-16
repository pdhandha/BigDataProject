from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import re

def check_datatype(input):
    if input is "" or input is " ":
        return "NULL"
    else:
        return type(input)

def validity(x):
    if x is "" or x is " ":
        return True
    else:
        attempt_list = ["COMPLETED","ATTEMPTED"]
        x = x.upper()
        if x not in attempt_list:
            return False
        else :
            return True

def clean_invalid_data(header, data):
    filtered_data = data.filter(lambda x: (x == header) or (validity(x[10])))
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
    # columnData = lines.map(lambda x: (x[0], x[10], check_datatype(x[10]), validity(x[10])))
    # #columnData = columnData.filter(lambda x: x[3] == "VALID") #This line is used to filter the data and remove all invalid.
    # columnData.saveAsTextFile("col11.out")
    #
    # lines = lines.map(lambda x: (validity(x[10]), 1)).reduceByKey(lambda x, y: x + y).collect()
    # lines = sc.parallelize(lines)
    # lines.saveAsTextFile("col11Stats.out")

    sc.stop()
