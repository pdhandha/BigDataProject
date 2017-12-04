from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import re
import datetime


def check_datatype(input):
    if input is "" or input is " ":
        return "NULL"
    else:
        return type(input)

def validity(x):
    if x is "" or x is " ":
        return "NULL"
    else :
            y=x
            x=x.split(":")
            try:
                hour=int(x[0])
                minutes=int(x[1])
                seconds= int(x[2])
                if hour == 24 and minutes== 0 and seconds == 0:
                    hour=0
                try:
                        newTime= datetime.time(hour,minutes,seconds)
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

    columnData = lines.map(lambda x: (x[0], x[2], check_datatype(x[2]), validity(x[2])))
    #columnData = columnData.filter(lambda x: x[3] == "VALID") #This line is used to filter the data and remove all
    columnData.saveAsTextFile("col3.out")

    lines = lines.map(lambda x: (validity(x[2]), 1)).reduceByKey(lambda x, y: x + y).collect()
    lines = sc.parallelize(lines)
    lines.saveAsTextFile("col3Stats.out")

    sc.stop()
