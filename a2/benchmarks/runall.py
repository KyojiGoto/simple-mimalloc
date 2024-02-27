#!/usr/bin/env python3

import os.path
import subprocess
import sys

# Check for correct usage
def check_usage():
        if len(sys.argv) != 2:
                print("usage: %s <dir>" % sys.argv[0])
                print("    where <dir> is the directory containing the benchmark subdirectories")
                sys.exit(1)

    
def main():
        check_usage()
        errcount = 0
        benchdir = sys.argv[1]
        iters = 1;

        namelist = ["cache-scratch", "cache-thrash", "threadtest", "larson", "linux-scalability", "phong"]
        for name in namelist:
                print ("benchmark name = %s" % name)
                #print ("running runbench.py %s/%s %s %d" % (benchdir,name,name,iters))
                cmd = benchdir + '/runbench.py ' + benchdir + '/' + name + ' ' + name + ' ' + str(iters)
                run_it = subprocess.run(cmd, shell=True)
                errcount = errcount + run_it.returncode
                # Generate graphs
                cmd = benchdir + '/graphbench.pl ' + benchdir + '/' + name + ' ' + name
                run_it = subprocess.run(cmd, shell=True)
                print(run_it.returncode)
        return errcount

# Call main
result = main()
sys.exit(result)
