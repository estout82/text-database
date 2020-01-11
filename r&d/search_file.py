#!/usr/local/bin/python3

# f.xreadlines() -> python 2 used to return an iterable to the file (by line)
# line in f -> python3 files are iterable by default 

search = "three"

f = open("f.txt", "r+")

# get number of lines
numlines = 0
for line in f:
    numlines += 1

print("numlines: {}.".format(numlines))

f.seek(0)
i=0
while i < numlines:
    line = f.readline()
    line = line.replace("\n", "")
    if line == search:
        print("line {} matches search.".format(i))
        exit(0)
    i += 1

print("failed to find match.")
exit(1)