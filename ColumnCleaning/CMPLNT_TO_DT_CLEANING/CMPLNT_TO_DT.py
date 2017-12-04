from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import datetime

def check_datatype(x):
    if x is "" or x is " ":
        return "VALID"
    else :
        y=x
        x=x.split("/")
        try:
            year=int(x[2])
            month=int(x[0])
            day= int(x[1])
            try:
                newDate = datetime.datetime(year,month,day)
                return type(newDate)
            except :
                return type(y)
        except:
            return type(y)

def validity(x):
    if x is "" or x is " ":
        return "VALID"
    else :
        y=x
        x=x.split("/")
        try:
            year=int(x[2])
            month=int(x[0])
            day= int(x[1])
            if year >= 2006 and year <= 2015:
                try:
                    newDate = datetime.datetime(year, month, day)
                    return "VALID"
                except:
                    return "INVALID"
            else:
                return "INVALID"
        except:
            return "INVALID"

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    header = lines.first()
    lines = lines.filter(lambda x: x != header)

    columnData = lines.map(lambda x: (x[0], x[3], check_datatype(x[3]), validity(x[3])))
    #columnData = columnData.filter(lambda x: x[3] == "VALID") #This line is used to filter the data and remove all invalid entries
    columnData.saveAsTextFile("col4.out")

    lines = lines.map(lambda x: (validity(x[3]), 1)).reduceByKey(lambda x, y: x + y).collect()
    lines = sc.parallelize(lines)
    lines.saveAsTextFile("col4Stats.out")

    sc.stop()
