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
# resolution at the start: (1, 2, 4, 8, 16, ..., 2^n) where n is the number of countmin sketches
# we maintain in the hokusai datastructure. 
# as time gets further along, the resolutions increase in size. (i.e. 2^n -> larger and so on)
# we justify this because older frequency data is less important (exponentially) (section 3.1 description)
# to the current time interval we're trying to get the frequency info of
# (even more relevant to the trending discussion) 

# WE RETAIN 2^m minute resolution for the past 2^m minutes


class Hokusai(object):

