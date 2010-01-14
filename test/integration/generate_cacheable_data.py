#!/usr/bin/env python
"""Tool to generate random numbers with locality."""

import sys
import random
from optparse import OptionParser

class CacheableRandom(object):
    """Generate a series of pseudo random integers that respect locality.

    I.e. given a database with an LRU cache in front of it, which 
    accepts an integer as a key, requests made with the generated integers
    should (once the cache is warm) resut in cache hits for a configurable
    proportion of time.

    This is can be used to simulate production load of a web service which is
    keyed of a user id.

    """

    def __init__(self, nkeys, lower, upper, maxsize, hit_ratio):
        self._count = nkeys
        self._lower = lower
        self._upper = upper
        self._cache = []
        self._hit_ratio = hit_ratio
        self._maxsize = maxsize
        self._ptr = 0
        # To get repeatable results
        random.seed(92873498274) 

    def __iter__(self):
        return self

    def cache_size(self):
        """Cache size."""
        return len(self._cache)

    def next(self):
        """Generate the next integer unless we're at the end."""
        if self._count < 1:
            raise StopIteration
        self._count -= 1

        if len(self._cache) < self._maxsize:
            i = random.randint(self._lower, self._upper)
            self._cache.append(i)
            return i        

        if random.uniform(0, 1) < self._hit_ratio:
            return random.choice(self._cache)

        i = random.randint(self._lower, self._upper)
        self._cache[self._ptr] = i
        self._ptr = (self._ptr + 1) % self._maxsize
        return i

def main():
    """Parse the arguments, run the generator."""

    parser = OptionParser()
    parser.add_option("-n", "--nkeys", dest="nkeys", type="int",
                      default=1000000,
                      help="Number of keys to generate")
    parser.add_option("-l", "--lower", dest="lower", type="int",
                      default=0,
                      help="Lower limit on key values")
    parser.add_option("-u", "--upper", dest="upper", type="int",
                      default=55000000,
                      help="Upper limit on key values")
    parser.add_option("-m", "--maxsize", dest="maxsize", type="int",
                      default=20000,
                      help="Maximum size of the cache")
    parser.add_option("-r", "--ratio", dest="hit_ratio", type="float",
                      default=0.65,
                      help="Desired cache hit ratio")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose",
                      default=False,
                      help="Display additional info")
    options, _ = parser.parse_args()
    
    cacheable_random = CacheableRandom(options.nkeys,
                                       options.lower,
                                       options.upper, 
                                       options.maxsize,
                                       options.hit_ratio)
    cache_hits = 0
    seen = set()
    for i in cacheable_random:
        print i
        if options.verbose:
            if i in seen:
                cache_hits += 1
            else:
                seen.add(i)

    if options.verbose:
        pctage_hits = (float(cache_hits) / options.nkeys) * 100
        print >> sys.stderr
        print >> sys.stderr, "cache hits: ", cache_hits, \
            " %2.2f%% " % (pctage_hits)

if __name__ == '__main__':
    main()



                
                

                
             
            
    

    
    
        
    

    
