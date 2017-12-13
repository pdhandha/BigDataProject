from __future__ import print_function

import sys
import re
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))

    def check_datatype(input):
        if input == '':
            return "NULL"
        try:
            int(input)
            return "INT"
        except ValueError:
            return type(input)


    def validity(x):
        if x == '':
            return False
        mat = re.match('[1-9]{1}[0-9]*', x)
        if mat is not None:
            return True
        else:
            return False

    def clean_invalid_data(header, data):
        filtered_data = data.filter(lambda x: (x == header) or (validity(x[0])))
        return filtered_data


    def toCSVLine(data):
        return ','.join(data)

    header = lines.first()  # extract header

    lines = clean_invalid_data(header, lines)
    lines = lines.map(toCSVLine)
    lines.saveAsTextFile("crime_clean.csv")
    # lines = lines.filter(lambda x: x != header).map(lambda x: (x[0], check_datatype(x[0]), validity(x[0])))
    #
    # lines.saveAsTextFile("col1.out")
    #
    # validInvalid = lines.map(lambda x: (x[2], 1)).reduceByKey(lambda x, y: x + y).collect()
    #
    # validInvalid = sc.parallelize(validInvalid)
    #
    # validInvalid.saveAsTextFile("col1Stats.out")

    sc.stop()