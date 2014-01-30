#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author:       Adrien Péronnet
@contact:      adrien@apapa.fr
"""

from contextlib import contextmanager
import fcntl,functools
from copy import copy

@contextmanager
def opened_w_error(filename, mode="r"):
    """
    When manipulating a file, this context manager will take care of locking the file (from a process point of view).
    """
    f = open(filename, mode)
    #If we want to write into the file, we put an executive lock onto his file identity.
    lock_type=fcntl.LOCK_EX if "w" in mode else fcntl.LOCK_SH
    rv = fcntl.fcntl(f, lock_type)
    if rv != 0:
        raise Exception("Can't lock the file")
    try:
        yield f
    finally:
        fcntl.fcntl(f, fcntl.LOCK_UN)
        f.close()


#To support *kwargs, set key=str(*args)+str(**kwargs)
def memoize(f):
    cache = f.cache = {}
    @functools.wraps(f)
    def memoizer(*args, **kwargs):
        if args not in cache:
            cache[args] = f(*args, **kwargs)
        return copy(cache[args])
    return memoizer
