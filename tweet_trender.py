# COS 521 Final Project
# Algorithm Implementation 

import fib_heap as fh


# current tweet storage

# queue for holding hashtags from the stream
# (hashtag, timestamp pairs)
input_queue = []

# hash table
# map from hashtag to (frequency, ptr to heap) pairs
hashtag_freq = dict()

# max Fib heap
# decrease_key is amortized constant
# To benefit from Fibonacci heaps in practice, 
# you have to use them in an application where decrease_keys are incredibly frequent.
# we want to do k delete-max ops in the heap 
# http://stromberg.dnsalias.org/~strombrg/fibonacci-heap-mod/
print "this worked again"