import sys
import numpy as np

if __name__ == '__main__':
    with open(sys.argv[1], "rb") as f:
        for line in f.readlines():
            strpln = line.decode("utf-8").strip().replace("(","").replace(")","")
            tmp = strpln.split(",")
            tmp[2] = float(tmp[2])*1000
            print(tmp)