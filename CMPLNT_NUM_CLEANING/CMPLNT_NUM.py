from __future__ import print_function

import sys
import re
import string
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))


    def check_complaintNumber_datatype(input):
        try:
            int(input)
            return "INT"
        except ValueError:
            return type(input)


    def check_if_valid(x):
        if x == '':
            return "NULL"
        mat = re.match('[1-9]{1}[0-9]*', x)
        if mat is not None:
            return "VALID"
        else:
            return "INVALID"

    header = lines.first()

    lines = lines.filter(lambda x: x != header).map(lambda x: (x[0]))

    intAndOther = lines.map(lambda x: (x,check_complaintNumber_datatype(x))).filter(lambda x: x[1] == "INT")

    valid_Invalid_Null = lines.map(lambda x: (check_if_valid(x),1)).reduceByKey(lambda x,y: x+y).collect()

    valid_Invalid_Null = sc.parallelize(valid_Invalid_Null);

    column1 = lines.map(lambda x: (x, check_complaintNumber_datatype(x), check_if_valid(x)))

    column1.saveAsTextFile("col1.out")
    valid_Invalid_Null.saveAsTextFile("col1Stats.out")

    sc.stop()