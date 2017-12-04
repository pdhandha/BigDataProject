from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import math
import sys

class Point:
    def __init__(self,x,y):
        self.x = x
        self.y = y

Latmax=40.917577
Latmin=40.477399
Lonmin=-74.25909
Lonmax=-73.700009
def isInNYC(point):
    if(point.x<Latmin or point.x>Latmax):
      return False
    if(point.y<Lonmin or point.y> Lonmax):
      return False
    return True

def check(x):
    if x== "" or x== " "or x=="\t":
        return "NULL"
    x=x.strip("'")
    x=x.replace("(","")
    x=x.replace(")","")
    lat,lon=x.split(",")
    lat=lat.strip()
    lon=lon.strip()
    lat=float(lat)
    lon=float(lon)
    if isInNYC(Point(lat,lon)) :
        return "VALID"
    else:
        return "INVALID"
    print (lat,lon)

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    header = lines.first()
    lines = lines.filter(lambda x: x != header)

    columnData = lines.map(lambda x: (x[0], x[23],  check(x[23])))
    #columnData = columnData.filter(lambda x: x[2] == "VALID") #This line is used to remove the identified invalid entires from the column
    columnData.saveAsTextFile("col24.out")

    lines = lines.map(lambda x: (check(x[23]), 1)).reduceByKey(lambda x, y: x + y).collect()
    lines = sc.parallelize(lines)
    lines.saveAsTextFile("col24Stast.out")

    sc.stop()
