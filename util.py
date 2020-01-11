#!/usr/local/bin/python3

# function: strips \n from given string
# return: newline-less string on sucess, empty string on failure
def stripnewlines(string) -> str:
    if type(string) != str:
        print("ut > unable to strip new lines from {}. type is not string".format(string))
        return str()
    return string.replace("\n", "")

def output(msg, newline=True):
    if newline:
        print("db: {}".format(msg))
    else:
        print("db: ", end="")