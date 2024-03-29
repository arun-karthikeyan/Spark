#!/usr/bin/env python
#The code was originally run using Ipython
#The following code must be executed in pyspark console

def mapper_fileA(line):
    line = line.strip()
    vals = line.split(",")
    word = vals[0]
    count = int(vals[1])
    return (word, count)

def mapper_fileB(line):
    line = line.strip()
    vals = line.split(",")
    key = vals[0].split()
    return (key[1],key[0]+" "+vals[1])

RDD_fileA = sc.textFile("file:///home/cloudera/Spark/input/join1_FileA.txt")
RDD_fileB = sc.textFile("file:///home/cloudera/Spark/input/join1_FileB.txt")
fileB_joined_fileA = (RDD_fileB.map(mapper_fileB)).join(RDD_fileA.map(mapper_fileA))

fileB_joined_fileA.collect() # displays joined o/p
