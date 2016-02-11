from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="AirportRank")
ssc = StreamingContext(sc, 10)
ssc.checkpoint("checkpoint")

#lines = ssc.textFileStream("/user/otp")
kvs = KafkaUtils.createDirectStream(ssc, ["flights"], {"metadata.broker.list": "hdp-master:9092"})

def print_top_list(rdd):
  for (count, word) in rdd.take(10):
    print("%s: %i" % (word, count))
#  print '[%s]' % ', '.join(map(str, rdd.take(10)))

def updateFunc(new_values, last_sum):
  return sum(new_values) + (last_sum or 0)

lines = kvs.map(lambda x: x[1])
running_counts = lines.flatMap(lambda line: line.split(",")[4:6]).map(lambda apt: (apt, 1)).updateStateByKey(updateFunc)

#reduceByKey(lambda x, y: x + y)
top = running_counts.map(lambda x: (x[1],x[0])).transform(lambda rdd: rdd.sortByKey(False))
top.foreachRDD(print_top_list)
#top.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
