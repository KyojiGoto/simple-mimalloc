#!/usr/bin/env python3

import os.path
import subprocess
import sys
import configparser
import signal

# Check for correct usage
def check_usage():
        if len(sys.argv) != 4:
                print("usage: %s <dir> <name> <iters>" % sys.argv[0])
                print("    where <dir> is the directory containing the test executable and Results subdir,")
                print("    <name> is the base name of the test executable, and")
                print("    <iters> is the number of trials to perform.")
                sys.exit(1)

def main():
        check_usage()
        errcount = 0
        benchdir = sys.argv[1]
        benchname = sys.argv[2]
        iters = int(sys.argv[3])
        nthread = 8

        # If benchdir is not an absolute path, and doesn't already start with "."
        # add the "./"
        if not (benchdir.startswith('/') or benchdir.startswith('./')):
                benchdir = './' + benchdir

        print("=== dir = %s" % benchdir)
        print("=== iters = %d" % iters)

        #Ensure existence of benchdir/Results directory
        resultdir = benchdir + '/Results'
        if os.path.exists(resultdir):
                if not os.path.isdir(resultdir):
                        print("Error: %s exists but is not a directory!" % resultdir)
                        sys.exit(1)
        else:
                os.mkdir(resultdir, mode = 0o0755)

        # Initialize list of allocators to test.
        # uncomment the line corresponding to the allocators you want to run.
        #alloclist = ["libc", "kheap", "a2alloc"]
        #alloclist = ["libc", "kheap"]
        alloclist = ["libc"]
        #alloclist = ["a2alloc"]

        #Initialize from config file
        #Each benchmark dir must contain a 'config' file that sets 
        # maxtime and args for that benchmark.
        configfile = benchdir+'/config'
        if not os.path.exists(configfile):
                print("Error: configuration file %s does not exist!" % configfile)
                sys.exit(1)
        config = configparser.ConfigParser()
        config.read(configfile)
        limit = int(config['DEFAULT']['maxtime'])
                    
        # Loop over each allocator
        for allocator in alloclist:
                print("allocator name = %s" % allocator)
                # Create subdirectory for current allocator results
                allocresultdir = resultdir + '/' + allocator
                if not os.path.exists(allocresultdir):
	                os.mkdir(allocresultdir, 0o755)

                # Run tests for 1 to nthread threads
                for i in range(1, nthread+1):
                        print("Thread %d" % i)
                        resultfname = allocresultdir + '/' + benchname + '-' + str(i)
                        resultfobj = open(resultfname,"w")
                        benchfull = benchdir + '/' + benchname + '-' + allocator
                        cmd = [benchfull, str(i)]
                        cmd = cmd + config['DEFAULT']['args'].split()

                        for j in range(1, iters+1):
                                # run benchmark with default args and thread #
                                # sending all output to result file
                                # Set timeout from config file
                                try:
                                        print("Running '%s' with timeout %d" % (cmd, limit), flush=True)
                                        result = subprocess.check_call(cmd, timeout=limit, stdout=resultfobj, stderr=subprocess.STDOUT)
                                except subprocess.TimeoutExpired:
                                        print("Killed. Timed out.", file = resultfobj)
                                        resultfobj.flush()
                                        errcount = errcount + 1
                                except subprocess.CalledProcessError as e:
                                        print("Died. ", end = "", file = resultfobj)
                                        if e.returncode < 0:
                                                print(signal.Signals(-1*e.returncode).name, file = resultfobj)
                                        else:
                                                print(e.returncode, file = resultfobj)
                                        resultfobj.flush()
                                        errcount = errcount + 1
                                        
                        resultfobj.close()

        return errcount

# Call main
result = main()
sys.exit(result)
