#!/usr/local/bin/python3

l = [1, 2, 3, 4, 5]

# a weird way to iterator over lists (list comprehensions)
# - do this .... print(x)
# - for every value (x) in some iteratable object (l)
print("f = [print(x) for x in l]: ", end="")
f = [print("{}, ".format(x), end="") for x in l]
print("")

# prints the list of function objects
# - FIXME: list of None because print() returns None
print("f: {}.".format(f))

# generate a new list using the above method
print("x * x for x in l: {}.".format( [x * x for x in l] ))

def foo(nums):
    r = list()
    for i in nums:
        r.append(i * i)
    return r

def bar(nums):
    for i in nums:
        yield i * i

# will print the list because a list object is returned
print("foo(l): {}.".format(foo(l)))

# will print out the info for a generator object because a generator object is returned
print("bar(l): {}.".format(bar(l)))

 # prints '1' because next is called on the generator object returned by bar(l)
print("next(bar(l): {}".format( next(bar(l) )))

# also prints '1' because next is called on the generator object returned by bar(l)
# - a different generator object than before
print("next(bar(l): {}".format( next(bar(l) )))

# prints out the a list because foo() returns a list of values
regularlist = foo(l)
print("foo(l): {}".format(regularlist))

# prints our a generator object because bar() returns a generator object
genobject = bar(l)
print("bar(l): {}".format(genobject))

# prints '1' because next is called on the generator object returned by bar() (genobject)
one = next(genobject)
print("next(genobject): {}".format(one))

# prints '4' beccause next is called on the same generator object as previous
# - the saved state is returned to and the for-loop is resumed until next call to yield
# - when yield is called, then a new value is return and state is saved again
two = next(genobject)
print("next(genobject): {}".format(two))

# will print the list as it is iterated over
# - next() is called on the generator object returned by bar(l) each time the for-loop continues
print("for i in bar(l): ", end="")
for i in bar(l):
    print("{}, ".format(i), end="")
print("")

# convert generator to list
print("list(bar(l): {}".format( list(bar(l)) ))