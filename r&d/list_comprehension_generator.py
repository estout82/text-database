#!/usr/local/bin/python3

vals = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

# create a list comprehendsion ( straight up list object )
lc = [ x * x for x in vals ]

print("vals: {}".format(vals))
print("lc:   {}".format(lc))

# create a generator using a list comprehension
lcg = ( x * x for x in vals)
print("lcg:  {}".format(lcg))

# loop through and print the generator
print("for x in lcg: ", end = "")
for x in lcg:
    print("{}, ".format(x), end = "")
print("") # for newline

# alt syntax
print("alt implementation of for-loop: ", end = "")
[ print("{}, ".format(x), end = "") for x in vals ]
print("")