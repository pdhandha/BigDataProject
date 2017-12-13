from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys

if __name__ == "__main__":

    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))


    def check_datatype(input):
        if input is "" or input is " ":
            return "NULL"
        else:
            return type(input)

    def validity(x):
        if x is "" or x is " " :
            return False
        else:
            list_of_boroughs=['BRONX',"BROOKLYN","MANHATTAN","QUEENS","STATEN ISLAND"]
            if x not in list_of_boroughs:
                return False
            else :
                return True

    def clean_invalid_data(header, data):
        filtered_data = data.filter(lambda x: (x == header) or (validity(x[13])))
        return filtered_data

    def toCSVLine(data):
        return ','.join(data)

    header = lines.first()  # extract header

    lines = clean_invalid_data(header, lines)
    lines = lines.map(toCSVLine)
    lines.saveAsTextFile("crime_clean.csv")

    # header = lines.first()    #extract header
    #
    # lines = lines.filter(lambda x: x != header)
    #
    # column13_filtering = lines.map(lambda x: (x[0], x[13], check_datatype(x[13]), validity(x[13])))
    # #column13_filtering = column13_filtering.filter(lambda x: x[3] == "VALID") #This line is used to filter the data and remove all invalid entries invalid.
    # column13_filtering.saveAsTextFile("col14.out")
    #
    # valid_Invalid_Null = lines.map(lambda x: (validity(x[13]), 1)).reduceByKey(lambda x, y: x + y).collect()
    # valid_Invalid_Null = sc.parallelize(valid_Invalid_Null)
    # valid_Invalid_Null.saveAsTextFile("col14Stats.out")


    sc.stop()