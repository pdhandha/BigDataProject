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
        return False
    elif re.match('[1-9]+', x):
        return True
    else:
        return False


def convert(x):
    try:
        return validity(x)
    except:
        return "NULL"

def clean_invalid_data(header, data):
    filtered_data = data.filter(lambda x: (x == header) or (validity(x[14])))
    return filtered_data


def toCSVLine(data):
    return ','.join(data)

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    header = lines.first()

    lines = clean_invalid_data(header, lines)
    lines = lines.map(toCSVLine)
    lines.saveAsTextFile("crime_clean.out")

    # lines = lines.filter(lambda x: x != header)
    #
    # columnData = lines.map(lambda x: (x[0], x[14], check_datatype(x[14]), validity(x[14])))
    # #columnData = columnData.filter(lambda x: x[3] == "VALID") #This line is used to filter the data and remove all invalid. In our code we simply mark the row as valid or invalid
    # columnData.saveAsTextFile("col15.out")
    #
    # lines = lines.map(lambda x: (validity(x[14]), 1)).reduceByKey(lambda x, y: x + y).collect()
    # lines = sc.parallelize(lines)
    # lines.saveAsTextFile("col15Stats.out")

    sc.stop()