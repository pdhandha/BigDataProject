from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import re
import datetime


def check_datatype(x):
    if x is "" or x is " ":
        return "NULL"
    else:
        y = x
        x = x.split("/")
        try:
            year = int(x[2])
            month = int(x[0])
            day = int(x[1])
            try:
                newDate = datetime.datetime(year, month, day)
                return "VALID"
            except:
                return "INVALID"
        except:
            return "INVALID"


def validity(x):
    if x is "" or x is " ":
        return False
    else:
        y = x
        x = x.split("/")
        try:
            year = int(x[2])
            month = int(x[0])
            day = int(x[1])
            try:
                datetime.datetime(year, month, day)
                return True
            except:
                return False
        except:
            return False


def clean_invalid_data(header, data):
    filtered_data = data.filter(lambda x: (x == header) or (validity(x[5])))
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
    lines.saveAsTextFile("crime_clean.csv")

    sc.stop()
