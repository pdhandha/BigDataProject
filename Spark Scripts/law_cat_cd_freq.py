from __future__ import print_function

from pyspark import SparkContext
from csv import reader
import sys


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))

    def validity(x):
            if x is "" or x is " ":
                return "INVALID"
            else :
                return x


    header = lines.first()    #extract header

    lines = lines.filter(lambda x: x != header)
    # lines = lines.filter(lambda x: (str(x[13]) == "BROOKLYN"))

    valid_Invalid_Null = lines.map(lambda x: (validity(x[11]), 1)).reduceByKey(lambda x, y: x + y)

    valid_Invalid_Null = valid_Invalid_Null.filter(lambda x: x[0] != "INVALID")


    counts = valid_Invalid_Null.map(
        lambda x: str(x[0]).replace("'", "").replace('(', '').replace(')', '') + '\t' + str(x[1]))

    counts.saveAsTextFile("law_cat_cd_freq_all.out")
    valid_Invalid_Null = valid_Invalid_Null.collect()
    sc.stop()