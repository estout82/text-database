#!/usr/local/bin/python3

l = [1, 2, 3, 4, 5]

print("weird for loop: {}".format(i for i in l))

print("list: {}.".format(l))

# option one
t = tuple(l)
print("tuple: {}.".format(l))

# option two
t = tuple(i for i in l)
print("tuple: {}.".format(t))

# FIXME: this does not work
#t = tuple(*l,)
print("tuple: {}.".format(t))