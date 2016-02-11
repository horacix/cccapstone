from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from operator import itemgetter

sc = SparkContext(appName="CarrierRank")
ssc = StreamingContext(sc, 10)
ssc.checkpoint("checkpoint")


def print_top_list(rdd):
  for (count, word) in rdd.take(10):
    print("%s: %i" % (word, count))

def updateFunc(new, last):
  new_sum = 0
  new_count = 0
  if last:
    new_sum = last[0]
    new_count = last[1]
  for val in new:
    new_sum = new_sum + val[0]
    new_count = new_count + val[1]
  return (new_sum, new_count)

def parse_line(line):
  vals = itemgetter(2,9)(line.split(","))
  return (vals[0], float(vals[1]))

kvs = KafkaUtils.createDirectStream(ssc, ["flights"], {"metadata.broker.list": "hdp-master:9092"})

lines = kvs.map(lambda x: x[1]).filter(lambda x: x.find('false') < 0)
data = lines.map(parse_line)

running_sumcount = data.transform(lambda rdd: rdd.combineByKey(lambda value: (value, 1), lambda x, value: (x[0] + value, x[1] + 1), lambda x, y: (x[0] + y[0], x[1] + y[1]))) #.updateStateByKey(updateFunc)
data.pprint()
running_sumcount.pprint()

#top = running_counts.map(lambda x: (x[1],x[0])).transform(lambda rdd: rdd.sortByKey(False))
#top.foreachRDD(print_top_list)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
