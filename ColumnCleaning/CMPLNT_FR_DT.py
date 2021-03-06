from __future__ import print_function

from pyspark import SparkContext
from csv import reader
import sys, datetime

if __name__ == "__main__":

    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))

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
                    return type(newDate)
                except:
                    return type(y)
            except:
                return type(y)


    def validity(x):
        if x is "" or x is " ":
            return False
        else:
            x = x.split("/")
            try:
                year = int(x[2])
                month = int(x[0])
                day = int(x[1])
                if year >= 2006 and year <= 2015:
                    try:
                        newDate = datetime.datetime(year, month, day)
                        return True
                    except:
                        return False
                else:
                    return False
            except:
                return False

    def clean_invalid_data(header, data):
        filtered_data = data.filter(lambda x: (x == header) or (validity(x[1])))
        return filtered_data

    def toCSVLine(data):
        return ','.join(data)

    header = lines.first()  # extract header

    lines = clean_invalid_data(header, lines)
    lines = lines.map(toCSVLine)
    lines.saveAsTextFile("crime_clean.out")

    # header = lines.first()  # extract header
    #
    # lines = lines.filter(lambda x: x != header).map(lambda x: (x[0],x[1],check_datatype(x[1]),validity(x[1])))
    # lines.saveAsTextFile("col2.out")
    #
    # validInvalid = lines.map(lambda x: (x[3],1)).reduceByKey(lambda x,y:x+y).collect()
    #
    # validInvalid = sc.parallelize(validInvalid)
    #
    # validInvalid.saveAsTextFile("col2Stats.out")

    sc.stop()