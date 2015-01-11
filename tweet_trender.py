# COS 521 Final Project
# Algorithm Implementation 

import fib_heap as fh
from collections import default_dict
import history as h
import key_functions

# when parsing tweets, need to make sure 
# the tweet sending model (from file)
# takes into account times. 

# (so we fake the time sending, basically)
# (if we want to actually implement realtime,
#  need a time counter while getting tweets)



class SmartTrendPredictor:
	# update the hashtag frequency dict and heap data structures
	# new_data is in the format (hashtag, timestamp)
	def update_datastructs(new_data):
		hashtag, timestamp = new_data
		if hashtag in self.hashtag_freq:
			# update the frequency
			curr_freq = self.hashtag_freq[hashtag][0]
			curr_freq += 1
			self.hashtag_freq[hashtag][0] = curr_freq

			# get the ptr and update the heap 
			ptr = self.hashtag_freq[hashtag][1]
			entry = self.heap_ptrs[ptr]
			new_priority = (curr_freq + 0.0)/ self.hokusai[hashtag]
			self.heap.decrease_key(entry, new_priority)
		else:
			# build the ptr to the heap
			priority = 1./self.hokusai[hashtag]
			ptr_val = self.heap.enqueue(hashtag, priority)
			self.heap_ptrs[self.curr_index] = ptr_val
			# build hashtag_freq entry
			self.hashtag_freq[hashtag] = [1, self.curr_index]
			# new pointer
			self.curr_index += 1

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
		# holds hashtags from the last y minutes
		self.hashtag_freq = dict()
		# max Fib heap
		# decrease_key is amortized constant
		# To benefit from Fibonacci heaps in practice, 
		# you have to use them in an application where decrease_keys are incredibly frequent.
		# we want to do k delete-max ops in the heap 
		# http://stromberg.dnsalias.org/~strombrg/fibonacci-heap-mod/
		# this is a min heap, so enter all keys as negative to get the max heap.
		self.heap = fh.Fibonacci_heap()

		# current heap ptr value
		self.curr_index = 0
		# dict from index values (numbers from m to n) to the heap value
		self.heap_ptrs = defaultdict(lambda:None)

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



