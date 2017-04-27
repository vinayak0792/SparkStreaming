import json
from kafka import KafkaConsumer
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from geopy.geocoders import Nominatim


def evaluate(text):
	sA = SentimentIntensityAnalyzer()
	polarity = sA.polarity_scores(text)
	if(polarity["compound"] > 0):
		return "positive"
	elif(polarity["compound"] < 0):
		return "nagative"
	else:
		return "neutral"


def get_locations(locations):
	geolocator = Nominatim()
	location = geolocator.geocode(locations)
	if location != None:
		return str(location.latitude) +"," + str(location.longitude)
	else:
		return "12.9791198,77.5912997"



def get_president(text):
	if "obama" in text.lower():
		return "obama"
	elif "trump" in text.lower():
		return "trump"
	else:
		return "trump"


def sentimentAnalysis(ssc):
	es = Elasticsearch()
	es_write_conf = { "es.resource" : "tsd/mem","es.input.json" : "true" } 
	kstream = KafkaUtils.createDirectStream(ssc, topics = ['twitter'], kafkaParams = {'metadata.broker.list': 'localhost:9092'})
	tweets  = kstream.map(lambda line: json.loads(line[1]))
	sentiAnalysis = tweets.map(lambda line: (line,evaluate(line["text"])))
	result2 = result1.map(lambda line: (None, line))
	result2.foreachRDD(lambda line : line.saveAsNewAPIHadoopFile(
            path="-", 
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=es_write_conf))
	ssc.start()
	ssc.awaitTermination()
	

def main():
	con = SparkConf()
	sc = SparkContext(conf=con)
	ssc = StreamingContext(sc, 10)
	ssc.checkpoint("checkpoint")
	#consumer(ssc)
	sentimentAnalysis(ssc)
	
if __name__=="__main__":
    main()