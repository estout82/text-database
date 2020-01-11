#!/usr/local/bin/python3

def count() -> int:
    i = 0
    while i < 10:
        print("i: {}".format(i))
        i += 1
    return i

x = count()
print("x: {}".format(x))