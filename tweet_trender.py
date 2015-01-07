# COS 521 Final Project
# Algorithm Implementation 

import fib_heap as fh
from collections import default_dict

import key_functions


class SmartTrendPredictor:
	# update the hashtag frequency dict and heap data structures
	# new_data is in the format (hashtag, timestamp)
	def update_datastructs(new_data):
		hashtag, timestamp = new_data
		if hashtag in self.hashtag_freq:
			# update the frequency
			self.hashtag_freq[hashtag][0] += 1
		else:
			# build the ptr to the heap
			# calculate key
			self.heap.enqueue()

	def __init__(self, data_file):
		self.tweets = defaultdict(list)
		self.inverted_tweets_idx = defaultdict(list)
		# current tweet storage
		'''
		# queue for holding hashtags from the stream
		# (hashtag, timestamp pairs)
		self.input_queue = []
		'''
		# map from hashtag to (frequency, ptr to heap) pairs
		self.hashtag_freq = dict()
		# max Fib heap
		# decrease_key is amortized constant
		# To benefit from Fibonacci heaps in practice, 
		# you have to use them in an application where decrease_keys are incredibly frequent.
		# we want to do k delete-max ops in the heap 
		# http://stromberg.dnsalias.org/~strombrg/fibonacci-heap-mod/
		# this is a min heap, so enter all keys as negative to get the max heap.
		self.heap = fh.Fibonacci_heap()


		# Hokusai data structure 
		self.hokusai = 0

		with open(data_file) as f:
			for line in f:
				try:
	                id_, timestamp, hashtag = line.strip().split(',')
	                tweet_dt = parser.parse(timestamp)
	            except Exception, e:
	                print e
	                continue
	            self.tweets[hashtag.lower()].append(tweet_dt)
	            self.inverted_tweets_idx[tweet_dt].append(hashtag.lower())



