#!/usr/local/bin/python3

import io
import os
import sys

import util as ut

# file format:
# - 8 bytes for current key
# - 8 bytes how many rows
# - 8 bytes key, name, serial

dbfile = None
dbkeygen = -1
dbnumrows = -1
dbnumfields = 5
dbdata = dict()

def safeexit(code):
    closedb(dbfile)
    exit(code)

# function - opens a db file 
# filename - name of file to open
# return - None on fail, a file object on sucess
def opendb(filename):
    if not os.path.exists(filename):
        ut.output("dbfile does not exist. please create it.")
        return None

    try:
        file = open(filename, "a+")
    except FileNotFoundError:
        ut.output("file does not exist.")
        return None

    global dbdata
    global dbkeygen
    global dbnumrows

    # TODO: add error checking here

    file.seek(0, 0)
    tmp = file.readlines(2)
    tmp[0] = tmp[0].replace('\n', "")
    dbkeygen = int(tmp[0])

    tmp[1] = tmp[1].replace('\n', "")
    dbnumrows = int(tmp[1])

    lines = file.readlines()

    i = 0
    for line in lines:
        line = line.replace('\n', "")
        s = line.split(",")
        if len(s) != 5:
            ut.output("line {} was not parsed sucessfully.".format(i))
        else:
            dbdata[int(s[0])] = (s[1], s[2], s[3], s[4])

    return file

def closedb(file):
    if file != None:
        if not file.closed:
            file.close()

    dbdata.clear()
    return 0

def savedb():
    # ensure db is open
    if dbfile == None or dbfile.closed:
        ut.output("db file is closed. please open a db.")
        return 1

    dbfile.truncate

def keygen() -> int:
    global dbkeygen

    if dbkeygen == -1:
        ut.output("invalid key seed. unable to gen key.")
        return -1
    else:
        dbkeygen += 1
        return dbkeygen

def createrow(key, data) -> int:
    global dbnumrows

    # ensure db is open
    if dbfile == None or dbfile.closed:
        ut.output("db file is closed. please open a db.")
        return 1

    if key == -1:
        ut.output("invalid key specified for new row. aborting...")
        return 1

    dbdata[key] = data
    dbnumrows += 1

    return 0

def savedb():
    if dbfile == None or dbfile.closed:
        ut.output("db file is closed. please open a db.")
        return 1

    # delete the whole file and start over
    dbfile.seek(0)
    dbfile.truncate()

    # write metadata
    dbfile.seek(0)
    dbfile.write('{}\n'.format(dbkeygen))
    dbfile.write('{}\n'.format(dbnumrows))

    i = 0
    for key in dbdata.keys():
        print("writing row {} to the file...".format(i))
        i += 1
        data = dbdata[key]
        dbfile.write("{},{},{},{},{}\n".format(key, data[0], data[1], data[2], data[3]))

        dbfile.flush()

    return 0

def createdb(filename) -> int:
    # don't overwrite existing file w/o permission
    if os.path.exists(filename):
        ut.output("file exists... overwrite?")
        a = input("(y/n) > ")
        if a == "y" or a == "yes":
            ut.output("overwriting file...")
        else:
            ut.output("aborting creation...")
            return 1

    try:
        tmpfile = open(filename, "x")
    except:
        ut.output("error creating file.")
        return 1
    
    # keygen number
    tmpfile.write('0\n')
    # num rows in file
    tmpfile.write('0\n')

    tmpfile.close()

    return 0

def searchdb(value, field: int):
    global dbfile
    global dbnumfields

    if dbfile == None or dbfile.closed:
        ut.output("db is closed.")
        return None
    if field >= dbnumfields or field < 0:
        ut.output("{} is not a field in this db.")
        return None

    if field == 0: # search based on keys
        return dbdata[value]
    else: # search based on fields in the tuple
        # find all hits for value and put them into r
        r = list()
        for k in dbdata.keys():
            if dbdata[k][field - 1] == value:
                r.append((k, dbdata[k]))
        if len(r) == 0:
            return None # if no hits were found return None
        else:
            return r # if hits were found, return the list

def main():
    global dbfile

    filename = ""

    # if args.. attempt to open file

    if len(sys.argv) > 1:
        filename = sys.argv[1]
        dbfile = opendb(filename)

    if dbfile == None:
        ut.output("no db file is open.")
    else:
        ut.output("db file {} is open".format(filename))
    
    while True:
        line = input("> ")
        split = line.split(" ")
        commandstr = split[0]

        if commandstr == "new":
            key = keygen()

            if key == -1:
                ut.output("unable to gen key for new row. aborting...")
                continue

            name = input("name > ")
            serial = input("serial > ")
            location = input("location > ")
            comment = input("comment > ")

            createrow(key, (name, serial, location, comment))

        elif commandstr == "delete":
            key = input("key > ")
            if dbdata[key] == None:
                ut.output("{} is not a key in the db.".format(key))
            else:
                dbdata.pop(key)

        elif commandstr == "update":
            line = input("key > ")
            # convert line str to int
            try: key = int(line)
            except: 
                ut.output("{} is not a valid key".format(key))
                continue

            if dbdata[key] == None:
                ut.output("{} is not a key in the db.".format(key))
            else:
                data = dbdata[key]

                fields = input("fields to update > ") # get what fields need to be modified
                fields = fields.split(" ")  # split into the names
                name = data[0]
                serial = data[1]
                location = data[2]
                comments = data[3]
                for f in fields:
                    v = input("{} > ".format(f))
                    if f == "name":
                        name = v
                    elif f == "serial":
                        serial = v
                    elif f == "location":
                        location = v
                    elif f == "comments":
                        comments = v
                    else:
                        ut.output("{} is nota valid field id")

                # create a new tuple with modified values
                t = (name, serial, location, comments)
                dbdata[key] = t

        elif commandstr == "search":
            # ensure a field was specified to preform search
            if len(split) <= 1:
                ut.output("please specify a field to search.")
            else:
                # switch based on field specified
                field = split[1]
                hits = None
                if field == "key":
                    key = input("key > ")
                    try: key = int(key) 
                    except: ut.output("{} must be an integer.".format(key))
                    hits = searchdb(key, 0)
                elif field == "name":
                    name = input("name > ")
                    hits = searchdb(name, 1)
                elif field == "serial":
                    serial = input("serial > ")
                    hits = searchdb(serial, 2)
                elif field == "location":
                    location = input("location > ")
                    hits = searchdb(location, 3)
                elif field == "comments":
                    comments = input("comments > ")
                    hits = searchdb(comments, 4)
                else:
                    ut.output("{} is not a searchable field.".format(field))
                    continue

                if hits == None:
                    ut.output("no hits found.")
                else:
                    for h in hits:
                        ut.output("{}: {}".format(h[0], h[1]))

        elif commandstr == "list":
            if len(dbdata) == 0:
                ut.output("db is empty.")
            else:
                for k in dbdata.keys():
                    print("{}: {}".format(k, dbdata[k]))

        elif commandstr == "save":
            if savedb() != 0:
                ut.output("unable to save db.")
            else:
                ut.output("db saved.")

        elif commandstr == "numrows":
            if dbfile == None or dbfile.closed:
                ut.output("no db file is open.")
            else:
                ut.output("numrows: {}".format(dbnumrows))
                
        elif commandstr == "open":
            if dbfile != None and not dbfile.closed:
                ut.output("a db is already open... continue?")
                a = input("(y/n) > ")
                if a != "y" and a != "yes":
                    ut.output("aborting open...")
                    continue
            
            fn = input("filename > ")
            dbfile = opendb(fn)
            if dbfile == None:
                ut.output("db was not opened sucessfully")
            else:
                ut.output("db opened.")

        elif commandstr == "close":
            if closedb(dbfile) != 0:
                ut.output("db not closed sucessfully.")
            else:
                ut.output("db closed.")

        elif commandstr == "create":
            fn = input("filename > ")
            if createdb(fn) != 0:
                ut.output("db was not created sucessfully.")
            else:
                ut.output("db created.")

        elif commandstr == "exit":
            safeexit(0)

        else:
            ut.output("{} is not a valid command.".format(commandstr))

main()