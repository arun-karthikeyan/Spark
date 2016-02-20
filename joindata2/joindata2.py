#!/usr/bin/env python
#The code was originally run using Ipython
#The following code must be executed in pyspark console

def split_show_views(line):
    line = line.strip()
    current_row = line.split(',')
    show = current_row[0]
    views = int(current_row[1])
    return (show, views)

def split_show_channel(line):
    line = line.strip()
    line = line.split(',')
    return (line[0], line[1])

def extract_channel_views(show_views_channel):
    show = show_views_channel[0]
    views = show_views_channel[1][0]
    channel = show_views_channel[1][1]
    return (channel, views)

def add_views(a, b):
    return a+b



show_views_file = sc.textFile('file:///home/cloudera/Spark/joindata2/input/join2_gennum?.txt') #Loads all matching files into show_views_file RDD

show_views = show_views_file.map(split_show_views) #Mapper for fileA

show_channel_file = sc.textFile('file:///home/cloudera/Spark/joindata2/input/join2_genchan?.txt') #Loads all matching files into show_channel_file RDD

show_channel = show_channel_file.map(split_show_channel)# Mapper for fileB

joined_data = show_views.join(show_channel) #Joining Both Files

channel_views = joined_data.map(extract_channel_views) #Final Map

result = channel_views.reduceByKey(add_views) #Reducer
