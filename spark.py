from operator import add
from pyspark import SparkConf, SparkContext
import time

sc = SparkContext()

raw_links = sc.textFile('s3://xxx/data/links-simple-sorted.txt')
raw_titles = sc.textFile('s3://xxx/data/titles-sorted.txt')

# creating keys for the title
title_dict = raw_titles.zipWithIndex().map(lambda x: (x[0], x[1] + 1))

def string_to_array(line):
    fields = line.split(":")
    keys = int(fields[0])
    outlinks_string = fields[1]
    outlinks_array = outlinks_string.split(" ")
    return(keys, outlinks_array)

links_wide = raw_links.map(string_to_array)
out_links = links_wide.flatMapValues(lambda x: [int(i) for i in x if i != '']).map(lambda x: (x[0],x[1]))

# Initialization
hub_score = title_dict.map(lambda x: (int(x[1]), 1.0))
auth_score = title_dict.map(lambda x: (int(x[1]), 1.0))

for i in range(8):

    print '\n\n====================\n\n'
    print 'UPDATING ITERATION NUMBER : ' + str(i)
    print '\n\n====================\n\n'
    auth_score = out_links.join(hub_score).map(lambda x: (x[1][0],x[1][1])).reduceByKey(lambda a,b: a+b).cache()
    hub_score = out_links.map(lambda x: (x[1],x[0])).join(auth_score).map(lambda x: (x[1][0],x[1][1])).reduceByKey(lambda a,b: a+b).cache()
    
    print '\n\n====================\n\n'
    print 'NORMALIZATION ITERATION NUMBER : ' + str(i)
    print '\n\n====================\n\n'
    auth_norm = (auth_score.mapValues(lambda x: x*x).map(lambda x: x[1]).reduce(add))**(0.5)
    hub_norm = (hub_score.mapValues(lambda x: x*x).map(lambda x: x[1]).reduce(add))**(0.5)
    auth_score = auth_score.mapValues(lambda x: (x/auth_norm)).cache()
    hub_score = hub_score.mapValues(lambda x: (x/hub_norm)).cache()

    auth_score.collect()
    hub_score.collect()

auth_score = auth_score.join(title_dict.map(lambda x: (x[1],x[0]))).map(lambda x: (x[1][1], x[1][0]))
hub_score = hub_score.join(title_dict.map(lambda x: (x[1],x[0]))).map(lambda x: (x[1][1], x[1][0]))

for i in (auth_score.takeOrdered(20, key=lambda x: -x[1])):
    print str(i[0]) + ' : ' + str(i[1])
    
for i in (hub_score.takeOrdered(20, key=lambda x: -x[1])):
    print str(i[0]) + ' : ' + str(i[1])
