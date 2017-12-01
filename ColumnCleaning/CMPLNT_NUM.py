from _future_ import print_function

import sys
import re
import string
from operator import add
from pyspark import SparkContext
from csv import reader

if _name_ == "_main_":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))


    def check_complaintNumber_datatype(input):
        if not input:
            return "NULL"
        try:
            int(input)
            return "INT"
        except ValueError:
            return type(input)


    def check_valid_sematic_type(value):
        isMatched = re.match('[1-9]{1}[0-9]*', value)
        if isMatched is not None:
            return "Complaint Number"
        else:
            return "Other"


    def check_if_valid(x):
        if x == '':
            return "NULL"
        mat = re.match('[1-9]{1}[0-9]*', x)
        if mat is not None:
            return "VALID"
        else:
            return "INVALID"


    header = lines.take(1)

    lines = lines.filter(lambda x: x != header).map(lambda x: (x[0]))

    column1 = lines.map(lambda x: (x, check_complaintNumber_datatype(x), check_valid_sematic_type(x), check_if_valid(x)))

    column1.saveAsTextFile("column1.out")

    sc.stop()