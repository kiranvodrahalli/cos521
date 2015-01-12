# COS 521 Final Project
# Algorithm Implementation 

import fib_heap as fh
from collections import defaultdict
from collections import deque
import history as h
import key_functions
from dateutil import parser
from datetime import timedelta
from copy import deepcopy

# when parsing tweets, need to make sure 
# the tweet sending model (from file)
# takes into account times. 

# (so we fake the time sending, basically)
# (if we want to actually implement realtime,
#  need a time counter while getting tweets)


# this is being run on FILES of previous twitter data
# with (hashtag, timestamp) format
# that's why we use the timestamp to calculate time
# instead of actual time as we would if it were real-time



# HOW TO USE:
# SmartTrendPredictor takes as input a file of tweets, 
# in the format (id, timestamp, hashtag)
# the assumption is that the tweets are in chronological order.
# the trend predictor builds a datastructure that evalutes 
# the top k tweets for that specific file. 
# we want to implement time snapshots
class SmartTrendPredictor(object):

	def __init__(self, data_file):

		# maintains list of hashtags
		# that have occurred in the past time unit
		# (say, 1 day -- can fix this however we want)
		# it's set to zero again after it gets added to history
		self.data_block = []

		# number of seconds in a day
		# a day is a block, can modify this
		# this is for the history datastructure
		# THIS NEEDS TO BE A TIME DELTA FOR COMPARISON
		self.block_threshold = timedelta(days=1, hours=0, minutes=0, seconds=0)

		# datetime of the time the last block ended at - we take times with reference to that
		self.end_of_last_block = None

		# 3 hours for now
		# the resolution of the heap/ frequency hash table
		# THIS NEEDS TO BE A TIME DELTA SINCE WE DO ADDITION
		self.y_res = timedelta(hours=3, minutes=0, seconds=0)

		self.tweets = defaultdict(list)
		self.inverted_tweets_idx = defaultdict(list)

		# current tweet storage
		
		# queue for holding hashtags from the stream
		# (hashtag, timestamp pairs)
		# the right is the most recent
		# the left is the oldest
		self.input_queue = deque()
		
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

		# similar to Hokusai data structure 
		# pick some parameters here
		# n = 10, m = 1000 (size of hash tables), d = 100 (# of tables)
		self.hist = h.History(10, 1000, 100)

		# first line bool
		# used for checking if its the first line in the file
		# when we parse the file
		isFirstLine = True

		# not finished
		with open(data_file) as f:
			for line in f:
				try:
					id_, timestamp, hashtag = line.strip().split(',')
					tweet_dt = parser.parse(timestamp)
					if isFirstLine:
						# 'last blocks' start at the beginning of the file
						self.end_of_last_block = tweet_dt
						isFirstLine = False
					self.update_datastructs((hashtag, tweet_dt))
				except Exception, e:
					print e
					continue
				self.tweets[hashtag.lower()].append(tweet_dt)
				self.inverted_tweets_idx[tweet_dt].append(hashtag.lower())

	# update the hashtag frequency dict and heap data structures and queue
	# new_data is in the format (hashtag, timestamp)
	def update_datastructs(self, new_data):
		hashtag, curr_dt = new_data
		# update the history
		# check timestamp and stuff to build current datablocks
		# diff should be an integer in seconds, maybe
		diff = curr_dt - self.end_of_last_block

		# aggregate only for every block timeunit
		if diff > self.block_threshold:
			self.hist.aggregate_unit(self.data_block)
			self.data_block = []
			self.end_of_last_block = curr_dt
		else:
			self.data_block.append(hashtag)
			# update the present structure in hist
			# don't worry, this will get cleared when
			# we add the whole block, and get re-added from the start
			self.hist.update_present_only(hashtag)

		# update all other structs
		if hashtag in self.hashtag_freq:
			# update the frequency
			curr_freq = self.hashtag_freq[hashtag][0]
			curr_freq += 1
			self.hashtag_freq[hashtag][0] = curr_freq	

			# get the ptr and update the heap 
			ptr = self.hashtag_freq[hashtag][1]
			entry = self.heap_ptrs[ptr]
			val = entry.get_value()
			old_priority = entry.get_priority()
			# negative since we're using a min heap	
			# query value should never be 0
			new_priority = (-1)*(curr_freq + 0.0)/ self.hist.query(hashtag)
			if new_priority >= old_priority:
				# delete enqueue to avoid decrease_key operation
				# when we're not actually decreasing the key
				self.heap.delete(entry)
				ptr_val = self.heap.enqueue(val, new_priority)
				# we simply update the ptr in the heap ptr
				# the value in hashtag_freq does not change, since
				# its the same ptr value
				self.heap_ptrs[ptr] = ptr_val
			else:
				# we can decrease the key since new_priority < old_priority
				self.heap.decrease_key(entry, new_priority)
		else: # hashtag not yet seen
			# build the ptr to the heap
			# negative since we have a min heap
			# query value should never be 0
			priority = (-1)*1./self.hist.query(hashtag)
			ptr_val = self.heap.enqueue(hashtag, priority)
			self.heap_ptrs[self.curr_index] = ptr_val
			# build hashtag_freq entry
			# current frequency is 1
			self.hashtag_freq[hashtag] = [1, self.curr_index]
			# new pointer
			self.curr_index += 1

		# update the queue
		# (appending to the right, most recent)
		self.input_queue.append(new_data)

		# check first thing in the queue (left)
		# while the time of the element of the
		# queue + y is less than current time
		# (that means it's too old)
		# remove it from the "current" data structures
		# (the heap, the freq hashtable, the queue)
		# (since we delete from the left, the index to look at is always 0)
		while self.input_queue[0][1] + self.y_res < curr_dt:
			hashtag_old, old_dt = self.input_queue.popleft()
			# decrease the frequency of that hashtag by 1
			new_freq = self.hashtag_freq[hashtag_old][0] - 1
			if new_freq > 0:
				# update hashtag frequency table
				self.hashtag_freq[hashtag_old][0] = new_freq
				# update heap 
				ptr = self.hashtag_freq[hashtag_old][1]
				entry = self.heap_ptrs[ptr]
				val = entry.get_value()
				old_priority = entry.get_priority()
				new_priority = (-1) * (new_freq + 0.0) / self.hist.query(hashtag_old)
				if new_priority >= old_priority:
					# delete enqueue to avoid decrease_key operation
					# when we're not actually decreasing the key
					self.heap.delete(entry)
					ptr_val = self.heap.enqueue(val, new_priority)
					# we simply update the ptr in the heap ptr
					# the value in hashtag_freq does not change, since
					# its the same ptr value
					self.heap_ptrs[ptr] = ptr_val
				else:
					self.heap.decrease_key(entry, new_priority)
			else: # frequency is 0, we delete this hashtag
				ptr = self.hashtag_freq[hashtag_old][1]
				# delete the pointer to this entry from the heap, since its frequency is 0
				self.heap.delete(self.heap_ptrs[ptr])
				# delete ptr from heap_ptrs
				del self.heap_ptrs[ptr]
				# delete the entry from the hashtable
				del self.hashtag_freq[hashtag_old]

	# the top hashtags in the min-heap
	# (most negative)
	def get_topk_hashtags(self, k):
		# what we will return
		# list of entries (have priority and value)
		best_nodes = []
		for i in range(k):
			top_node = self.heap.dequeue_min()
			val = top_node.get_value()
			priority = top_node.get_priority()
			#print 'Priority: ' + str(-1*priority)
			#print 'Value: ' + str(val)
			best_nodes.append((priority, val))
		for node in best_nodes:
			self.heap.enqueue(node[1], node[0])
		return sorted(best_nodes, reverse=True)

	def print_all_keys(self):
		iter_heap = deepcopy(self.heap)
		key_counts = defaultdict(int)
		print 'Printing all Key Priorities: '
		while iter_heap.m_size > 0:
			node = iter_heap.dequeue_min()
			key = -1*node.get_priority()
			key_counts[key] += 1
		for key in key_counts:
			print 'count of ' + str(key) + ': ' + str(key_counts[key])


def test():
	stp = SmartTrendPredictor('tweet_data')
	stp.print_all_keys()
	print '============================'
	top5 = stp.get_topk_hashtags(5)
	top10 = stp.get_topk_hashtags(10)
	top15 = stp.get_topk_hashtags(15)
	top4000 = stp.get_topk_hashtags(4000)
	print '----------------------------'
	print 'Top 5 in order:'
	for tag_pair in top5:
		print str(tag_pair[1]) + ", " + str(tag_pair[0])
	print '----------------------------'
	print 'Top 10 in order:'
	for tag_pair in top10:
		print str(tag_pair[1]) + ", " + str(tag_pair[0])
	print '----------------------------'
	print 'Top 15 in order:'
	for tag_pair in top15:
		print str(tag_pair[1]) + ", " + str(tag_pair[0])
	print '----------------------------'
	print 'Top 4000 in order:'
	for tag_pair in top4000:
		print str(tag_pair[1]) + ", " + str(tag_pair[0])
	print '----------------------------'




