from __future__ import print_function

from pyspark import SparkContext
from pyspark.sql import Row
from csv import reader
import sys, datetime

if __name__ == "__main__":

    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))

    def check_DATE_datatype(x):
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


    def check_if_valid(x):
        if x is "" or x is " ":
            return "INVALID"
        else:
            x = x.split("/")
            try:
                year = int(x[2])
                month = int(x[0])
                day = int(x[1])
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



    header = lines.first()  # extract header

    lines = lines.filter(lambda x: x != header).map(lambda x: (x[0],x[1],check_DATE_datatype(x[1]),check_if_valid(x[1])))
    lines.saveAsTextFile("col2.out")

    validInvalid = lines.map(lambda x: (x[3],1)).reduceByKey(lambda x,y:x+y).collect()

    validInvalid = sc.parallelize(validInvalid)

    validInvalid.saveAsTextFile("col2Stats.out")

    sc.stop()