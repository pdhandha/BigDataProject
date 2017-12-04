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
        return "NULL"
    else:
        return "VALID"

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    header = lines.first()
    lines = lines.filter(lambda x: x != header)

    columnData = lines.map(lambda x: (x[0], x[7], check_datatype(x[7]), validity(x[7])))
    #columnData = columnData.filter(lambda x: x[3] == "VALID") #This line is used to filter the data and remove all invalid entries
    columnData.saveAsTextFile("col9.out")

    lines = lines.map(lambda x: (validity(x[7]), 1)).reduceByKey(lambda x, y: x + y).collect()
    lines = sc.parallelize(lines)
    lines.saveAsTextFile("col9Stats.out")

    sc.stop()
