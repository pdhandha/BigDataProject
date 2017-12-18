from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime

def get_hours(timedata):
    if(timedata=="" or timedata == " "):
        return "INVALID"
    if(timedata.split(":")[0] == "24"):
        timedata = timedata.replace("24","00")
    timedata = datetime.datetime.strptime(timedata, '%H:%M:%S').time()
    return datetime.datetime.strptime(str(timedata.hour) + ":00:00", "%H:%M:%S").time()

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    rows = lines.mapPartitions(lambda x: reader(x))
    hour_of_day = rows.filter(lambda x: (str(x[13]) == "BROOKLYN"))
    hour_of_day = hour_of_day.map(lambda x:(get_hours(x[2]), 1))
    hour_of_day_count = hour_of_day.reduceByKey(add)

    hour_of_day_count = hour_of_day_count.filter(lambda x: x[0] != "INVALID")

    # convert Voilation_count to RDD and dictionary for formatting
    # result = sc.parallelize(Voilation_count)
    counts = hour_of_day_count.map(
        lambda x: str(x[0]).replace("'", "").replace('(', '').replace(')', '') + '\t' + str(x[1]))

    counts.saveAsTextFile("brooklyn_time.out")

    sc.stop()