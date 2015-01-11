# COS 521 Final Project
# Implementation of the history data structure
# Author: Kiran Vodrahalli
# using Hokusai paper as inspiration with a few modifications
# citation: http://www.auai.org/uai2012/papers/231.pdf

# note that Corollary 2 is what allows us to add CM-sketches
# from different periods of time (that are disjoint)

# time interval lengths of powers of two match up since they
# are disjoint from each other


# we can compress time intervals together into one count min sketch (with less accuracy)
# as we go further into the past, so that we don't need to maintain all of it
# (we still retain approximate counts, we just push them together)

# resolution means "the amount of time for which we are counting the data"
# 2^m minute resolution means that we're keeping approximate frequency counts
# for 2^m minutes 

# building Hokusai means that we keep a number of countmin sketches with variable 
# resolution: (1, 2, 4, 8, 16, ..., 2^m) where m is the number of countmin sketches
# we maintain in the hokusai datastructure. 
# WE RETAIN 2^m minute resolution for the past 2^m minutes
# we justify this because older frequency data is less important (exponentially) (section 3.1 description)
# to the current time interval we're trying to get the frequency info of
# (even more relevant to the trending discussion) 

# we add an extra CMSketch on top of Hokusai to keep track of our weighted sums

from countminsketch import CountMinSketch as CMSketch
from copy import deepcopy


# sum two disjoint count-min sketches
# put the sum into M1, leave M2 alone
def sketch_sum(M1, M2):
	if M1.m != M2.m:
		print "Sketches don't align on hashtable length.\n"
		return
	elif M1.d != M2.d:
		print "Sketches don't align on # of hashtables.\n"
		return
	else:
		result_sketch = CMSketch(M2.m, M2.d)
		for i in range(M2.d):
			for j in range(M2.m):
				new_val = M1.val_at(i, j) + M2.val_at(i, j)
				result_sketch.update(i, j, new_val)
		return result_sketch


# multiply each value in the sketch by some factor
# we don't update the sketch, we return a new one
def sketch_scalar_product(M, c):
	result_sketch = CMSketch(M.m, M.d)
	for i in range(M.d):
		for j in range(M.m):
			new_val = c*M.val_at(i, j)
			result_sketch.update(i, j, new_val)
	return result_sketch


# ADD PRINTING FUNCTIONS LATER FOR DEBUGGING

# data structure designed based on Hokusai algorithm
# Keeps track of the approximate frequencies, weighting
# older frequencies with exponential decay
class History(object):
	def __init__(self, n, m, d):
		# time counter (update for each new unit)
		self.t = 0
		# n is number of CM sketches
		self.n = n 
		# m is size of array for each hash function
		self.m = m
		# d is number of hash functions
		self.d = d
		# present is a count-min sketch containing
		# sub-unit time counts of indexes. 
		self.present = CMSketch(m, d)
		# ready is a t/f value to determine whether or not
		# to use the present as a score while the aggregate weighted score
		# is being computed
		self.ready = False
		# use a CM-sketch to keep track of aggregate weighted score
		# A = sum{j = 1 to log T} (M^j / 2^j)
		# (we add the present ourselves)
		# keep track of A at every time interval
		# initialized to zero
		self.aggregate_score = CMSketch(m, d)
		# n count-min sketches 
		# we retain resolutions 1, 2, 4, ..., 2^n
		# move to next sketch (update curr_sketch) when 
		# time unit filled = 2^i (its position in the list)
		self.cm_sketch_list = []
		for i in range(n):
			self.cm_sketch_list.append(CMSketch(m, d))

	# data_block is a block of data, presented as an iterable object
	# the block of data consists of data that arrived in a single time unit
	# implements algorithm 2 from the paper
	# this structures maintains n CM-sketches, M0, M1, ..., Mn
	# M0 always holds [t-1, t] where t is current time
	# M1 always holds [t - tmod2 - 2, t - tmod2]
	# ...
	# Mn always holds [t - tmod(2^n) - 2^n, t - tmod(2^n)]
	# for t = 8, for example:
	# M0: [7, 8]
	# M1: [6, 8]
	# M2: [4, 8]
	# M3: [0, 8]
	# rest: 0
	def aggregate_unit(self, data_block):
		# update time once per unit
		self.t += 1
		# we use this to keep track of the current time unit
		# convert the data_block into a CM sketch
		accumulator = CMSketch(self.m, self.d)
		# add each hashtag in the data_block to the CM sketch
		# while this data is coming in, we maintain a separate
		# data structure with the exact frequencies that we can
		# query for exact frequencies. 
		# with frequency 1 for each appearance
		for data in data_block:
			accumulator.add(data, 1)
			# update present as we update the accumulator
			self.present.add(data, 1)

		self.ready = False
		# we update the whole structure with M_bar
		# we calculate l: 
		# l = max over all i such that (t mod 2^i) == 0
		# efficient -- takes log t time to find at worst
		def find_l(t):
			l = 0
			if t == 0:
				return l
			while t % 2 == 0:
				l += 1
				t = t/2
			return l

		# go up to the index that is find_l + 1, or the max index
		# if find_l + 1 >= to it
		for i in range(min(find_l(self.t) + 1, self.n)):
			# now we want to add the appropriate value: A + 1/2^(i)(M_bar - M^j)
			# M_bar - M^j
			difference = sketch_sum(accumulator, sketch_scalar_product(self.cm_sketch_list[i], -1))
			# A = A + (1/2)^i difference
			self.aggregate_score = sketch_sum(self.aggregate_score, 
									sketch_scalar_product(difference, pow(0.5, i)))
			# temporary storage
			T = deepcopy(accumulator)
			# aggregate into accumulator for next round
			accumulator = sketch_sum(accumulator, self.cm_sketch_list[i])
			# set the value
			self.cm_sketch_list[i] = T
		# now we're ready to use CM-sketch values
		self.ready = True
		# reset the present now that we're done with one time block
		self.present = CMSketch(self.m, self.d)

	# we want to put these values into its own count-min sketch, (call it A)
	# updated in sync so as to not waste log T time summing
	# for each query.
	# this value will provide a key for our heap
	def query_slow(self, x):
		return self.present.query(x) + sum(pow(0.5, i) * self.cm_sketch_list[i].query(x) for i in range(self.n))

	# using a CMSketch to keep track of the score
	# note that we stored the 'scores' we calculated in CM-sketch
	# therefore it will pick the minimum of these
	# this is exactly equivalent to doing the sum over the minimums since we added termwise
	# (used matrix addition and scalar multiplication)
	def query(self, x):
		if self.ready:
			return self.aggregate_score.query(x)
		else: # only if we're not ready 
			return self.present.query(x)


# test the history functions
def test():
	d1 = ['a', 'b', 'c']
	d2 = ['a', 'd', 'e']
	d3 = ['a', 'f', 'g']
	d4 = ['k', 'l', 'm']
	d5 = ['n', 'o', 'p', 'c']
	d6 = ['b', 'h', 'i']
	ds = [d1, d2, d3, d4, d5, d6]
	terms = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'k', 'l', 'm', 'n', 'o', 'p']
	h = History(5, 10, 30)
	for i in range(len(ds)):
		print '======================================'
		print 'Added Data #' + str(i)
		h.aggregate_unit(ds[i])
		for term in terms:
			print 'Term: ' + str(term)
			print 'Slow Query: ' + str(h.query_slow(term))
			print 'Query: ' + str(h.query(term))
			print '--------------------------'
		




		
