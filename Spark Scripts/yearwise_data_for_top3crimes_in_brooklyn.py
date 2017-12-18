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

    printdata = year_num.map(lambda x: x[9])
    print(printdata.collect())

    year_num_topCrime = year_num.filter(lambda x: (str(x[9])) == "ASSAULT 3")
    year_num_topCrime = year_num_topCrime.map(lambda x:(get_year(x[1]), 1))
    year_count_topCrime = year_num_topCrime.reduceByKey(add)
    topCrime_counts = year_count_topCrime.map(
        lambda x: str(x[0]).replace("'", "").replace('(', '').replace(')', '') + '\t' + str(x[1]))
    topCrime_counts.saveAsTextFile("topCrime_brooklyn_year_crime_freq.out")

    year_num_secondTopCrime = year_num.filter(lambda x: (str(x[9])) == "WEAPONS POSSESSION 3")
    year_num_secondTopCrime = year_num_secondTopCrime.map(lambda x: (get_year(x[1]), 1))
    year_count_secondTopCrime = year_num_secondTopCrime.reduceByKey(add)
    secondTopCrime_counts = year_count_secondTopCrime.map(
        lambda x: str(x[0]).replace("'", "").replace('(', '').replace(')', '') + '\t' + str(x[1]))
    secondTopCrime_counts.saveAsTextFile("secondTopCrime_brooklyn_year_crime_freq.out")

    year_num_thirdTopCrime = year_num.filter(lambda x: (str(x[9])) == "VIOLATION OF ORDER OF PROTECTI")
    year_num_thirdTopCrime = year_num_thirdTopCrime.map(lambda x: (get_year(x[1]), 1))
    year_count_thirdTopCrime = year_num_thirdTopCrime.reduceByKey(add)
    thirdTopCrime_counts = year_count_thirdTopCrime.map(
        lambda x: str(x[0]).replace("'", "").replace('(', '').replace(')', '') + '\t' + str(x[1]))
    thirdTopCrime_counts.saveAsTextFile("thirdTopCrime_brooklyn_year_crime_freq.out")

    sc.stop()
