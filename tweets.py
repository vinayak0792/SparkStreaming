import json
import tweepy
import threading, logging, time
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from kafka import KafkaProducer




class MyListener(StreamListener):

	def __init__(self,api):
		self.api = api
		self.producer = KafkaProducer(bootstrap_servers='localhost:9092')

	def on_data(self, data):
		msg = data.encode('utf-8') 
		try:
			if "\"location\":null" not in data:
				#print msg
				self.producer.send('twitter', msg)
				return True
		except BaseException as e:
			print(str(e))
			return False

	def on_error(self, status):
		print(status)
		return True

#Neccessary credentials required to aunthenticate the connection to access twitter from the API.
consumer_key = #'set your consumer_key'
consumer_secret = #'set your consumer_secret'
access_token = #'set your access_token'
access_secret = #'set your access_Secret'
authorize = OAuthHandler(consumer_key, consumer_secret)
authorize.set_access_token(access_token, access_secret)
api = tweepy.API(authorize)


twitter_stream = Stream(authorize, MyListener(api))
twitter_stream.filter(track=['#trump','#obama'])
        			

