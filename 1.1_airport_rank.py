from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="AirportRank")
ssc = StreamingContext(sc, 10)
ssc.checkpoint("checkpoint")

#lines = ssc.textFileStream("/user/otp")
#kvs = KafkaUtils.createDirectStream(ssc, ["flights"], {"metadata.broker.list": "hdp-master:9092"})
numStreams = 8
kafkaStreams = [KafkaUtils.createStream(ssc, 'hdp-slave2:2181', "spark-streaming-consumer", {'flights': 1}) for _ in range (numStreams)]
kvs = ssc.union(*kafkaStreams)

def print_top_list(rdd):
  print ("======")
  for (count, word) in rdd.take(10):
    print("%s: %i" % (word, count))

def updateFunc(new_values, last_sum):
  return sum(new_values) + (last_sum or 0)

lines = kvs.map(lambda x: x[1]).cache()
running_counts = lines.flatMap(lambda line: line.split(",")[4:6]).map(lambda apt: (apt, 1)).updateStateByKey(updateFunc)

#reduceByKey(lambda x, y: x + y)
top = running_counts.map(lambda x: (x[1],x[0])).transform(lambda rdd: rdd.sortByKey(False))
top.foreachRDD(print_top_list)
#top.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
