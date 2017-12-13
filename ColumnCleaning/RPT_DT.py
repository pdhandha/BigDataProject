from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import re
import datetime

def check_datatype(x):
    if x is "" or x is " ":
        return "NULL"
    else :
        y=x
        x=x.split("/")
        try:
            year=int(x[2])
            month=int(x[0])
            day= int(x[1])
            try:
                newDate = datetime.datetime(year,month,day)
                return "VALID"
            except :
                return "INVALID"
        except:
            return "INVALID"

def validity(x):
    if x is "" or x is " ":
        return "NULL"
    else :
        y=x
        x=x.split("/")
        try:
            year=int(x[2])
            month=int(x[0])
            day= int(x[1])
            try:
                newDate = datetime.datetime(year,month,day)
                return "VALID"
            except :
                return "INVALID"
        except:
            return "INVALID"

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    header = lines.first()
    lines = lines.filter(lambda x: x != header)

    columnData = lines.map(lambda x: (x[0], x[5], check_datatype(x[5]), validity(x[5])))
    #columnData = columnData.filter(lambda x: x[3] == "VALID") #This line is used to filter the data and remove all invalid entries
    columnData.saveAsTextFile("col6.out")

    lines = lines.map(lambda x: (validity(x[5]), 1)).reduceByKey(lambda x, y: x + y).collect()
    lines = sc.parallelize(lines)
    lines.saveAsTextFile("col6Stats.out")

    sc.stop()
