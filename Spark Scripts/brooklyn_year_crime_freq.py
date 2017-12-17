from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime

def get_year(line):
    line = datetime.datetime.strptime(line,'%m/%d/%Y')
    return line.year

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    rows = lines.mapPartitions(lambda x: reader(x))
    year_num = rows.filter(lambda x: (str(x[13]) == "BROOKLYN"))
    year_num = year_num.map(lambda x:(get_year(x[1]), 1))
    year_count = year_num.reduceByKey(add)

    counts = year_count.map(
        lambda x: str(x[0]).replace("'", "").replace('(', '').replace(')', '') + '\t' + str(x[1]))

    counts.saveAsTextFile("brooklyn_year_crime_freq.out")

    sc.stop()