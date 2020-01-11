#!/usr/local/bin/python3

import os
import io
from collections import namedtuple

import util as ut

# Schema represents the data organization in a db
# fieldids: string identifying each field in a row
# numfields: number of fields in a row (not including key)
# fieldtypes: the types of the fields
#  - int, float, string
Schema = namedtuple("Schema", ["numfields", "fieldids", "fieldtypes"])

# Row represents an instance of a row in a db
#  - used in conjunction with a schema
#  - both are stored as members in the db class
Row = namedtuple("Row", ["key", "data"])

# class: a database that interfaces with a file
#  - organized in a schema
#  - rows that have been recently recalled are stored in cache
#  - rows that are modified are stored in cache
# schema: a schema object that describes the schema
# file: the file handle
# numrows: the number of rows in the db
# cache: a dict with the modified rows
#
# NOTE: a row in cache with null data is deleted
# NOTE: -1 is an invalid value for keygen

# TODO: add a metadata offset
# TODO: convert to binary file
# TODO: convert to a python type object in schema
class database:
    __NUM_METADATA_LINES = 5

    def __init__(self):
        self.__schema = None
        self.__file = None
        self.__keygen = -1
        self.__cache = dict()
        self.__numrows = 0
        pass

    # function: opens a db from a valid db file
    # filename: the name of the db file
    # return: 0 on sucess, 1 on fail, 2 on parse failure
    def open(self, filename) -> int:
        # does file exist?
        if os.path.exists(filename) != True:
            ut.output("{} does not exist.".format(filename))
            return 1
        
        # attempt to open file
        try:
            self.__file = open(filename, "r+")
        except:
            ut.output("unable to open file {}.".format(filename))
            return 1

        # parse numfields
        self.__file.seek(0, 0)
        tmp = self.__file.readline()
        tmp = tmp.replace("\n", "")

        # try to convert to int
        try:
            numfields = int(tmp)
        except:
            ut.output("an error occured while parsing 'numfileds'. check db file.")
            return 2

        # parse fieldids
        tmp = self.__file.readline()
        tmp = tmp.replace("\n", "")
        tmp = tmp.split(",")
        fieldids = list()
        for f in tmp:
            if f == "":
                continue
            fieldids.append(f)
        if len(fieldids) != numfields:
            ut.output("unable to parse fieldids. fieldsids and numfields does not match.")
            return 2

        # parse fieldtypes
        tmp = self.__file.readline()
        tmp = tmp.replace("\n", "")
        tmp = tmp.split(",")
        fieldtypes = list()
        for t in tmp:
            if t == "":
                continue
            
            # convert string to a python type object
            if t == "str":
                fieldtypes.append(str)
            elif t == "int":
                fieldtypes.append(int)
            elif t == "float":
                fieldtypes.append(float)

        if len(fieldtypes) != numfields:
            ut.output("unable to parse fieldtypes. fieldtypes and numfields does not match.")
            return 2

        # parse keygen
        tmp = self.__file.readline()
        tmp = tmp.replace("\n", "")

        # attempt to convert to int 
        try:
            keygen = int(tmp)
        except:
            ut.output("unable to parse keygen. check db file.")
            return 2

        # parse numrows
        tmp = self.__file.readline()
        tmp = tmp.replace("\n", "")

        # attempt to convert to int
        try:
            numrows = int(tmp)
        except:
            ut.output("unable to parse numrows. check db file.")
            return 2

        # fill out member data
        self.setschema(Schema(numfields, fieldids, fieldtypes))
        self.__numrows = numrows
        self.__keygen = keygen

        return 0

    def close(self) -> int:
        # is file already closed
        if self.__file != None or self.__file.closed != True:
            # flush cache
            if len(self.__cache) > 0:
                self.flush()
        self.__file.close()
        return 0

    def addrow(self, data) -> int:
        if type(data) != tuple:
            ut.output("unable to create row. data parameter must be of type 'tuple'")
            return 1

        key = self.__getnewkey()

        if key <= -1:
            ut.output("unable to create row. invalid key.")
            return 1

        self.__cache[key] = data
        self.__numrows += 1
        return 0

    def removerow(self, key):
        for r in self.__cache:
            if r.key == key:
                r.data = None
                self.numrows -= 1
                return 0

        self.__file.seek(0)
        for line in self.__file:
            if int(line.split(",")[0]) == key:
                self.__cache[key] = None
                self.numrows -= 1
                return 0

        ut.output("{} does not exist in the db.".format(key))
        return 1

    # function: find a row with designated key
    # return: the data tuple associated with key, None on fail/non-existent key
    def findkey(self, key):
        # preform checks
        if not self.isopen():
            ut.output("unable to find key {}. no db is open.")
            return None
        if self.isempty():
            ut.output("unable to find key {}. db is empty.")
            return None
        if not self.__validatefile():
            ut.output("unable to find key {}. file not valid.".format(key))
            return None

        # check cache first
        if key in self.__cache.keys():
            return self.__cache[key]
        # look in file if not in cache
        else:
            nl = self.__getfilenumlines()

            # loop through lines in file until a match is found
            self.__file.seek(0)
            i = 0
            while i < nl:
                if i in range(5): self.__file.readline(); i += 1; continue # skip metadata lines

                # read line in file
                l = self.__file.readline()
                l = ut.stripnewlines(l)
                sl = l.split(",")
                if key == int(sl[0]): # does key match?
                    r = self.__deserializerow(l)
                    if r == None:
                        ut.output("an error occured while deserailzing key {}. unable to retrive data.".format(key))
                        return None
                    return r.data
                i += 1

        return None

    def findval(self, field, val) -> dict:
        # preform checks
        if not self.isopen():
            ut.output("unable to find key {}. no db is open.")
            return None
        if self.isempty():
            ut.output("unable to find key {}. db is empty.")
            return None
        if not self.__validatefile():
            ut.output("unable to find key {}. file not valid.".format(key))
            return None

        # get the index of the desiered field
        fi = 0
        try:
            fi = self.__schema.fieldids.index(field)
        except ValueError:
            ut.output("unable to find val {}. field is invalid.".format(field))
            return None

        # ensure val is the same type as the field
        ft = self.__schema.fieldtypes[fi]
        if type(val) != ft:
            ut.output("unable to find val {}. field type is {} and val is {}.".format(val, type(val), ft))
            return 1
            
        # check all rows in file for val in field
        r = dict()
        nl = self.__getfilenumlines()
        self.__file.seek(0)
        i = 0
        while i < nl: # loop through lines
            if i in range(5): self.__file.readline(); i += 1; continue # skip metadata lines

            # read line in file
            l = self.__file.readline()
            l = ut.stripnewlines(l)
            row = self.__deserializerow(l)
            if row == None:
                ut.output("an error occured during findval op. failed to deserailze row {} in file.".format(i))
            else:
                if row.data[fi] == val:
                    r[row.key] = row.data
            i += 1

        # check cache for desiered val
        for k in self.__cache.keys():
            y = self.__cache[k][fi]
            if y == val:
                r[k] = self.__cache[k] # add cache entry to dict

        return r

    def findall(self) -> dict:
        # preform checks
        if not self.isopen():
            ut.output("unable to find all rows. db is not open.")
            return None
        if self.isempty():
            ut.output("unable to find all rows. db is empty.")
            return None
        if not self.__validatefile():
            ut.output("unable to find all rows. file not valid.")
            return None

        # iterate through file first
        nl = self.__getfilenumlines()
        self.__file.seek(0)
        r = dict()
        i = 0
        while i < nl:
            # TODO: possibley use a yield statement here
            if i in range(5): self.__file.readline(); i += 1; continue 

            l = self.__file.readline()
            l = ut.stripnewlines(l)
            row = self.__deserializerow(l)
            if row == None:
                ut.output("an error occured during findall op. failed to deserailze row {} in file.".format(i))
            else:
                r[row.key] = row.data

            i += 1

        # then cache and update any entries that are incorrect in file
        for k in self.__cache.keys():
            r[k] = self.__cache[k]

        return r

    def update(self, key, field, val):
        # check the cache
        if key in self.__cache.keys():
            # get data of row in list form
            l = list(self.__cache[key])

            # get the index of the desiered field
            fi = 0
            try:
                fi = self.__schema.fieldids.index(field)
            except ValueError:
                ut.output("unable to find val {}. field is invalid.".format(field))
                return None

            # ensure val is the same type as field
            ft = self.__schema.fieldtypes[fi]
            if type(val) != ft:
                ut.output("unable to update key {}. field and val are not the same type.")
                return 1

            # rewrite new data
            l[fi] = val
            self.__cache[key] = tuple(l)
        else: # check in file
            # find the line in file
            self.__file.seek(0)
            ln = 0 # line number
            lp = 0 # last pos of file
            for l in self.__file:
                if ln in range(5): self.__file.readline(); ln += 1; continue # skip metadata

                # get the key of the line
                ls = l.split(",")
                k = int()
                try:
                    k = int(ls[0])
                except:
                    ut.output("unable to update key {}. key in file not valid.".format(key))
                    return 1

                # is this the key we are looking for?
                if k == key:
                    # get the data in list form
                    r = self.__deserializerow(l)
                    if r == None:
                        ut.output("unable to update key {}. failed to deserialize row.")
                        return 1
                    
                    d = list(r.data)

                ln += 1
                lp = self.__file.tell()

                    
            # change data
            # delete key
            # re-add key
            ln += 1
            NotImplemented
        return None

    # TODO: uodate the search method. line by line and dump only updated rows into file
    # TODO: idea... make a row container (or cache line container so only dirty rows are thrown into file)
    def flush(self) -> int:
        if not self.isopen():
            ut.output("unable to flush cache. no db is open.")
            return 1
        if self.__iscacheempty():
            ut.output("unable to flush cache. cahce is empty.")
            return 1
        
        # lines in the new file
        nd = list()

        # itr thru every line in file
        nl = self.__getfilenumlines()
        self.__file.seek(0)
        i = 0
        while i < nl:
            if i in range(5): self.__file.readline(); i += 1; continue

            od = self.__file.readline()
            od = od.replace("\n", "")
            k = int(od.split(",")[0])
            
            # see if its in cache
            if k in self.__cache.keys():
                if self.__cache[k] == None:
                    self.__cache.popitem(k) # done with this cache line
                else:
                    # build new line from data
                    newline = self.__serializerow(k, self.__cache[k])
                    self.__cache.popitem(k) # done with this cache line
            else:
                nd.append(od)
                pass
            i += 1

        # clean up remaining cache
        for k in self.__cache.keys():
            if self.__cache[k] == None:
                pass
            else:
                # build data from cache line data
                d = self.__serializerow(k, self.__cache[k])
                nd.append(d)

        # clear cache
        self.__cache = dict()

        # clear file
        self.__file.seek(0)

        # write metadata to file ----------

        # numfields
        s = "{}\n".format(self.__schema.numfields)
        print(s, end="")
        self.__file.write(s)

        # fieldids
        for fieldid in self.__schema.fieldids:
            s = "{},".format(fieldid)
            print(s, end="")
            self.__file.write(s)
        self.__file.write("\n")

        # field types
        for ft in self.__schema.fieldtypes:
            s = str()
            if ft == str:
                s = "str,"
            elif ft == int:
                s = "int,"
            elif ft == float:
                s = "float,"
            self.__file.write(s)
        self.__file.write("\n")

        # keygen
        s = "{}\n".format(self.__keygen)
        self.__file.write(s)

        # num rows
        s = "{}\n".format(self.__numrows)
        self.__file.write(s)

        # write new data to file
        for x in nd:
            self.__file.write("{}\n".format(x))

        self.__file.flush()

        return 0

    # function: set the schema of this db
    # schema: the new schema
    # return: 1 on fail, 0 on sucess
    def setschema(self, schema) -> int:
        if self.__numrows > 0:
            ut.output("cannot change the schema of a non-empty db.")
            return 1
        self.__schema = schema
        return 0

    # function: get the current schema of the db
    # return: the current schema of the db (could be 'None')
    def getschema(self) -> Schema:
        return self.__schema

    # function: get the number of rows in the whole db
    # return: the current numrows in the db (could be 0)
    def getnumrows(self) -> int:
        return self.__numrows

    def isopen(self) -> bool:
        if self.__file == None or self.__file.closed:
            return False
        else:
            return True

    def isempty(self) -> bool:
        if self.__numrows <= 0:
            return True
        return False

    def __iscacheempty(self) -> bool:
        if len(self.__cache) <= 0:
            return True
        return False

    # function: serialize a row given the key and the data
    #  - assumes data conforms to the schema
    # return: a serialized string according to the schema
    def __serializerow(self, key, data) -> str:
        s = str(key)
        i = 0
        while i < self.__schema.numfields:
            s += "," + str(data[i])
            i += 1
        return s

    # function: create a Row from a string
    # return: a Row object with the dat from the string on sucess, None on fail
    def __deserializerow(self, string) -> Row:
        ss = string.split(",")
        
        # check schema
        if len(ss) != self.__schema.numfields + 1:
            ut.output("unable to deserialize row string \"{}\". invalid schema.".format(string))
            return None

        # parse key section
        k = 0
        try:
            k = int(ss[0])
            del ss[0] # we are done with this now
        except:
            ut.output("unable to parse key \"{}\". invalid format.".format(ss[0]))
            return None

        # parse data section
        d = []
        i = 0
        while i < self.__schema.numfields:
            if self.__schema.fieldtypes[i] == "int":
                try:
                    x = int(ss[0])
                    d.append(x)
                    del ss[0] # we are done with this now
                except:
                    ut.output("unable to parse data {} as int. invalid type.".format(ss[0]))
                    return None
            elif self.__schema.fieldtypes[i] == "float":
                try:
                    x = float(ss[0])
                    d.append(x)
                    del ss[0] # we are done with this now
                except:
                    ut.output("unable to parse data {} as float. invalid type.".format(ss[0]))
                    return None
            # any other type treat as string FIXME: add more types
            else:
                x = ss[0]
                d.append(x)
                del ss[0] # we are done with this now

            i += 1
        
        # create row named tuple
        return Row(k, d)

    # function: gets a new key according to the current keygen
    # return: a new & unused key on sucess, -1 on failure
    def __getnewkey(self) -> int:
        if self.__keygen == -1:
            ut.output("unable to gen key. invalid keygen.")
            return -1

        self.__keygen += 1
        return self.__keygen

    # function: get the numlines in the db file
    # return: numlines in file on sucess, 0 on fail/empty file
    def __getfilenumlines(self):
        if self.__file == None or self.__file.closed:
            ut.output("unable to get num lines in file. file closed.")
            return 0
        
        self.__file.seek(0)
        n = 0
        for l in self.__file:
            n += 1
        return n

    def __validatefile(self) -> bool:
        n = self.__getfilenumlines()
        if n == 0 or n < self.__NUM_METADATA_LINES:
            ut.output("an error ocurred while finding key: {}. invalid number of lines in file.".format(key))
            return False

        # TODO: check other things here as well

        return True

# function: creates a new db file
#  - will prompt for file overwrite
# filename: the name of the file to create
# return: a database object on sucess, None on fail
def createdb(filename) -> database:
    db = database()
    f = None

    # how many fields
    a = input("schema: how many fields > ")
    try: numfields = int(a)
    except:
        ut.output("{} is not a number.".format(numfields))
        return None

    # enter ids for all those fields
    fieldids = list()
    fieldtypes = list()
    for i in range(numfields):
        fieldid = input("field {} id >  ".format(i))
        fieldids.append(fieldid)

        fieldtype = input("field {} type > ".format(i))
        fieldtypes.append(fieldtype)

    # should we overwritte?
    if os.path.exists(filename):
        a = input("file exists... overwrite? (y/n) > ")
        if a != "y":
            ut.output("aborting...")
            return None
    
    # open file
    try: f = open(filename, "x")
    except:
        ut.output("an error occurred creating the db.")
        return 1

    # create db object
    db.setschema(Schema(numfields, fieldids, fieldtypes))
    db.file = f

    # write the schema to the file
    f.write("{}\n".format(db.getschema().numfields))
    for fid in db.getschema().fieldids:
        f.write("{},".format(fid))
    f.write("\n")
    for ftype in db.getschema().fieldtypes:
        f.write("{},".format(ftype))
    f.write("\n")
    f.flush()

    # dump db object to file
    db.flush()
    return None

# TEST SECTION
db = database()
db.open("poo.db")
m = "michael"
t = tuple(m)
db.addrow(t)
r = db.findval("name", "bob")
print(r)
r = db.findval("name", "michael")
print(r)
db.close()