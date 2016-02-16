from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from operator import itemgetter
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

sc = SparkContext(appName="ArrivalDelayAverages")
ssc = StreamingContext(sc, 1)
ssc.checkpoint("checkpoint")

def sendPartition(iter):
  cluster = Cluster(['hdp-master','hdp-slave1','hdp-slave2','hdp-slave3'])
  session = cluster.connect('coursera')
  insert = session.prepare("INSERT INTO source_destination_mean_delay (source, destination, mean_arrival_delay) VALUES (?, ?, ?)")
  batch = BatchStatement()
  for record in iter:
    batch.add(insert, record)
  session.execute(batch)
  cluster.shutdown()  

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
  vals = itemgetter(4,5,9)(line.split(","))
  return ("%s:%s" % (vals[0], vals[1]), float(vals[2]))

kvs = KafkaUtils.createDirectStream(ssc, ["flights"], {"metadata.broker.list": "hdp-master:9092"})

lines = kvs.map(lambda x: x[1]).filter(lambda x: x.find('false') < 0)
data = lines.map(parse_line).cache()
# From http://abshinn.github.io/python/apache-spark/2014/10/11/using-combinebykey-in-apache-spark/
running_sumcount = data.transform(lambda rdd: rdd.combineByKey(lambda value: (value, 1), lambda x, value: (x[0] + value, x[1] + 1), lambda x, y: (x[0] + y[0], x[1] + y[1]))).updateStateByKey(updateFunc)
#running_sumcount.pprint()

averages = running_sumcount.map(lambda (key, (total, count)): tuple(key.split(':') + [total / count]))
# averages.pprint()
# sent to Cassandra
averages.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
