from operator import add
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
#sqlContext = SQLContext(sc)
from pyspark.sql.types import *
import time

sc = SparkContext()
sqlContext = SQLContext(sc)

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

fields = [StructField('from_id', IntegerType(), True), StructField('to_id', IntegerType(), True)]
schema = StructType(fields)
schemaOutLinks = sqlContext.createDataFrame(out_links, schema)
schemaOutLinks.registerTempTable("out_links")

fields = [StructField('id', IntegerType(), True), StructField('name', StringType(), True)]
schema = StructType(fields)
schemaTitles = sqlContext.createDataFrame(title_dict.map(lambda x: (x[1],x[0])), schema)
schemaTitles.registerTempTable("titles")

fields = [StructField('id', IntegerType(), True), StructField('score', DoubleType(), True)]
schema = StructType(fields)
schemaHubScore = sqlContext.createDataFrame(hub_score, schema)
schemaHubScore.registerTempTable("hub_score")
schemaAuthScore = sqlContext.createDataFrame(auth_score, schema)
schemaAuthScore.registerTempTable("auth_score")
schemaAuthScore.cache()
schemaHubScore.cache()

for i in range(8):
    # Updating scores
    print '\n\n====================\n\n'
    print 'UPDATING ITERATION NUMBER : ' + str(i)
    print '\n\n====================\n\n'

    schemaAuthScore = sqlContext.sql("select to_id as id, sum(score) as score from (SELECT * FROM out_links join hub_score on out_links.from_id = hub_score.id)  as joined_table  group by to_id")
    schemaAuthScore.registerTempTable("auth_score")
    schemaAuthScore.cache()

    schemaHubScore = sqlContext.sql("select from_id as id, sum(score) as score from (SELECT * FROM out_links join auth_score on out_links.to_id = auth_score.id)  as joined_table  group by from_id")
    schemaHubScore.registerTempTable("hub_score")
    schemaHubScore.cache()

    # Normalization
    print '\n\n====================\n\n'
    print 'NORMALIZATION ITERATION NUMBER : ' + str(i)
    print '\n\n====================\n\n'
    auth_norm = schemaAuthScore.map(lambda x: x.score**2).reduce(lambda a,b: a + b)**0.5
    schemaAuthScore = sqlContext.sql("select id, score/" + str(auth_norm) + "as score from auth_score").cache()
    schemaAuthScore.registerTempTable("auth_score")
    schemaAuthScore.cache()

    hub_norm = schemaHubScore.map(lambda x: x.score**2).reduce(lambda a,b: a + b)**0.5
    schemaHubScore = sqlContext.sql("select id, score/" + str(hub_norm) + "as score from hub_score")
    schemaHubScore.registerTempTable("hub_score")
    schemaHubScore.cache()

    schemaAuthScore.collect()
    schemaHubScore.collect()

auth_score_output = sqlContext.sql("Select name, score from auth_score Join titles on auth_score.id = titles.id order by score desc limit 20")
hub_score_output = sqlContext.sql("Select name, score from hub_score Join titles on hub_score.id = titles.id order by score desc limit 20")


print auth_score_output.show()
print hub_score_output.show()






