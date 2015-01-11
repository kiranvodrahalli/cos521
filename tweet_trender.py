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


# this is being run on FILES of previous twitter data
# with (time, hashtag) format
# that's why we use the timestamp to calculate time
# instead of actual time as we would if it were real-time
class SmartTrendPredictor:
	# time_str is in format hh:mm:ss
	# IMPORTANT: time_str1 < time_str2 in time
	def process_time_diff(time_str1, time_str2):
		times1 = time_str1.split(':')
		times2 = time_str2.split(':')
		hr1, hr2, m1, m2, s1, s2 = times1[0], times2[0], times1[1], times2[1], times1[2], times2[2]
		# in seconds
		time_diff = 0
		if hr1 > hr2: # only possible between 11 pm and 12 am, since we're on 24 hour clock
			hr2 += 24
		timetot1 = hr1*60*60 + m1*60 + s1
		timetot2 = hr2*60*60 + m2*60 + s2
		return timetot2 - timetot1

	# update the hashtag frequency dict and heap data structures
	# new_data is in the format (hashtag, timestamp)
	def update_datastructs(self, new_data):
		hashtag, timestamp = new_data
		# update the history
		# check timestamp and stuff to build current datablocks
		# diff should be an integer in seconds, maybe
		diff = process_time_diff(self.end_of_last_block, timestamp)
		# aggregate only for every block timeunit
		if diff >= self.threshold:
			self.hist.aggregate_unit(self.data_block)
			self.data_block = []
			self.end_of_last_block = timestamp
		else: #otherwise, update
			self.data_block.append(hashtag)


		if hashtag in self.hashtag_freq:
			# update the frequency
			curr_freq = self.hashtag_freq[hashtag][0]
			curr_freq += 1
			self.hashtag_freq[hashtag][0] = curr_freq

			# get the ptr and update the heap 
			ptr = self.hashtag_freq[hashtag][1]
			entry = self.heap_ptrs[ptr]
			new_priority = (curr_freq + 0.0)/ self.hist.query(hashtag)
			self.heap.decrease_key(entry, new_priority)
		else:
			# build the ptr to the heap
			priority = 1./self.hist.query(hashtag)
			ptr_val = self.heap.enqueue(hashtag, priority)
			self.heap_ptrs[self.curr_index] = ptr_val
			# build hashtag_freq entry
			self.hashtag_freq[hashtag] = [1, self.curr_index]
			# new pointer
			self.curr_index += 1

	def __init__(self, data_file):

		# maintains list of hashtags
		# that have occurred in the past time unit
		# (say, 1 day -- can fix this however we want)
		# it's set to zero again after it gets added to history
		self.data_block = []

		# number of seconds in a day
		# a day is a block, can modify this
		self.threshold = 60*60*24

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
		# pick some parameters here
		# n = 10, m = 1000 (size of hash tables), d = 100 (# of tables)
		self.hist = h.History(10, 1000, 100)

		# first line bool
		isFirstLine = True
		# string of the time the last block ended at - we take times with reference to that
		self.end_of_last_block = ''

		# not finished
		with open(data_file) as f:
			for line in f:
				try:
	                id_, timestamp, hashtag = line.strip().split(',')
	                tweet_dt = parser.parse(timestamp)
	                if isFirstLine:
	                	self.end_of_last_block = timestamp
	                	isFirstLine = False
	            except Exception, e:
	                print e
	                continue
	            self.tweets[hashtag.lower()].append(tweet_dt)
	            self.inverted_tweets_idx[tweet_dt].append(hashtag.lower())



