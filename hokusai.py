# COS 521 Final Project
# Hokusai Implementation

# produced from http://www.auai.org/uai2012/papers/231.pdf

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

from countminsketch as CountMinSketch
from copy import deepcopy


# sum two disjoint count-min sketches
# put the sum into M1, leave M2 alone
def sketch_sum(M1, M2):
	if M1.m != M2.m:
		print "Sketches don't align.\n"
	elif M1.d != M2.d:
		print "Sketches don't align.\n"
	else:
		for i in range(M2.m):
			for j in range(M2.d):
				new_val = M2.val_at(i, j)
				M1.update(i, j, new_val)

class Hokusai(object):
	def __init__(self, n, m, d):
		# number of the CM sketches filled so far
		# <= n
		self.num_filled = 0
		# n is number of CM sketches
		self.n_ = n 
		# m is number of hash functions
		self.m_ = m
		# d is size of array for each hash function
		self.d_ = d
		# n count-min sketches 
		# we retain resolutions 1, 2, 4, ..., 2^n
		# contains pairs of (time unit filled, sketch)
		# move to next sketch (update curr_sketch) when 
		# time unit filled = 2^i (its position in the list)
		self.cm_sketch_train = []
		for i in range(n):
			self.cm_sketch_train.append([0, CountMinSketch(m, d)])

	# data_block is a block of data
	# gathered over a time unit
	def aggregate_unit(self, data_block):
		# we use this to keep track of the current time unit
		M_bar = CountMinSketch(m, d)
		# we use self.M_bar
		if self.num_filled < self.n_:
			self.num_filled += 1
		print 'need implementation of data parsing here'
		# use self.M_bar 
		# we update the whole structure with it
		# only use this after we have aggregated a unit
		# implement algorithm 2 from the paper

		for i in range(self.num_filled):
			# temporary storage
			T = deepcopy(M_bar)
			sketch_sum(M_bar, self.cm_sketch_train[i])
			self.cm_sketch_train[i] = T







		
