## DOWNLOADED FROM 
## https://github.com/rafacarrascosa/countminsketch
## (available on pip)
## MODIFIED BY KIRAN VODRAHALLI for purposes of hokusai

# -*- coding: utf-8 -*-
import hashlib
import array


# to note: take the min: we get a lower bound; taking max we would get upper bound

# what happens to the case when we have something we've never seen before, but it
# hashes? how is performance affected here?

# modify to take into account the ter "b" -- as in M_b
# for different locations in time, or whatever?



class CountMinSketch(object):
    """
    A class for counting hashable items using the Count-min Sketch strategy.
    It fulfills a similar purpose than `itertools.Counter`.

    The Count-min Sketch is a randomized data structure that uses a constant
    amount of memory and has constant insertion and lookup times at the cost
    of an arbitrarily small overestimation of the counts.

    It has two parameters:
     - `m` the size of the hash tables, larger implies smaller overestimation
     - `d` the amount of hash tables, larger implies lower probability of
           overestimation.

    An example usage:

        from countminsketch import CountMinSketch
        sketch = CountMinSketch(1000, 10)  # m=1000, d=10
        sketch.update("oh yeah")
        sketch.update(tuple())
        sketch.update(1, value=123)
        print sketch["oh yeah"]       # prints 1
        print sketch[tuple()]         # prints 1
        print sketch[1]               # prints 123
        print sketch["non-existent"]  # prints 0

    Note that this class can be used to count *any* hashable type, so it's
    possible to "count apples" and then "ask for oranges". Validation is up to
    the user.
    """
    # make m, d accessible
    m = 0
    d = 0

    def __init__(self, m, d):
        """ `m` is the size of the hash tables, larger implies smaller
        overestimation. `d` the amount of hash tables, larger implies lower
        probability of overestimation.
        """
        if not m or not d:
            raise ValueError("Table size (m) and amount of hash functions (d)"
                             " must be non-zero")
        self.m = m
        self.d = d
        self.n = 0
        self.tables = []
        for _ in xrange(d):
            # KIRAN'S EDIT: change "l" to "f" -- we want to allow
            # float values since we use it to also maintain
            # weighted counts (and still want to use the min procedure)
            table = array.array("f", (0 for _ in xrange(m)))
            self.tables.append(table)


    # expose the internal array to update for purposes of addition
    # - Kiran 
    # update table i, index j
    # i in [0, m); j in [0, d)
    def update(self, i, j, new_val):
        self.tables[i][j] = new_val
    # get val at i j
    def val_at(self, i, j):
        return self.tables[i][j]

    def _hash(self, x):
        md5 = hashlib.md5(str(hash(x)))
        for i in xrange(self.d):
            md5.update(str(i))
            yield int(md5.hexdigest(), 16) % self.m

    def add(self, x, value=1):
        """
        Count element `x` as if had appeared `value` times.
        By default `value=1` so:

            sketch.add(x)

        Effectively counts `x` as occurring once.
        """
        self.n += value
        for table, i in zip(self.tables, self._hash(x)):
            table[i] += value

    def query(self, x):
        """
        Return an estimation of the amount of times `x` has ocurred.
        The returned value always overestimates the real value.
        """
        return min(table[i] for table, i in zip(self.tables, self._hash(x)))

    def __getitem__(self, x):
        """
        A convenience method to call `query`.
        """
        return self.query(x)

    def __len__(self):
        """
        The amount of things counted. Takes into account that the `value`
        argument of `add` might be different from 1.
        """
        return self.n
