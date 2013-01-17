import argparse, time, Pyro4, sys, os
from datetime import timedelta

import config
from eris import Eris
from storage import Storage

SLEEP_PERIOD = 0.25
STATUS_RETRIES = 3

def start(args):
    Eris().start()
    proxy = Eris.getProxy()
    for _ in range(STATUS_RETRIES):
        try:
            if proxy.ping() == config.PNAME:
                break
        except Pyro4.errors.CommunicationError:
            time.sleep(SLEEP_PERIOD)
    status()
    
def stop(args):
    proxy = Eris.getProxy()
    try:
        proxy.stop()
    except Pyro4.errors.CommunicationError:
        pass
    for _ in range(STATUS_RETRIES):
        try:
            if proxy.ping() != config.PNAME:
                break
            time.sleep(SLEEP_PERIOD)
        except Pyro4.errors.CommunicationError:
            break
    status()
    try:
        with open(config.statusFile, "w"):
            pass
    except: 
        pass
    
def status(args = None):
    def printStatus(statusFile, pid = None, cpu = None, mem = None, uptime = None, du = None):
        status = (color("WORKING", "green")   if statusFile and pid else
                  color("NOT WORKING", "red") if not statusFile and pid is None else
                  color("UNKNOWN", "red"))
        print "Status: " + status
        if pid:
            print "    PID:     {}".format(pid)
            print "    CPU:     {:.1f}%".format(cpu)
            print "    MEM:     {:.1f}%".format(mem)
            print "    Storage: {} KiB".format(du)
            print "    Up-time: {}".format(str(timedelta(uptime.days, uptime.seconds, 0)))
            
    statusFile = False
    try:
        with open(config.statusFile, "r") as f:
            statusFile = f.read() == config.STATUS_LINE
    except:
        pass
    try:
        (pid, cpu, mem, uptime, du) = Eris.getProxy().status()
        printStatus(statusFile, pid, cpu, mem, uptime, du)
    except Pyro4.errors.PyroError:
        printStatus(statusFile)
        
def put(args):
    try:
        data = open(args.file, "r").read() if args.file else sys.stdin.read()
        packets = [(args.timestamp, data)]
        proxy = Eris.getProxy()
        proxy.put(packets)
    except IOError as e:
        print >> sys.stderr, e
    except Pyro4.errors.PyroError:
        print >> sys.stderr, "eris not available" 
        

def get(args):
    try:
        proxy = Eris.getProxy()
        packets = proxy.get(args.since, 0, args.limit)
        print "<packets>"
        for p in packets:
            print "<packet timestamp=\"{}\">\n{}\n</packet>".format(p[0], "  " + p[1].replace("\n", "\n  ")) 
        print "</packets>"
    except Pyro4.errors.PyroError:
        print >> sys.stderr, "eris not available"
        
def count(args):
    try:
        proxy = Eris.getProxy()
        c = proxy.count()
        print "{} packets in storage".format(c)
    except Pyro4.errors.PyroError:
        print >> sys.stderr, "eris not available"
        
def clean(args):
    try:
        Eris.getProxy().status()
        print >> sys.stderr, "eris must be stopped"
    except Pyro4.errors.PyroError:
        try:
            print "storage deleted"
            os.remove(Storage.dbFile())
        except:
            pass


def color(text, clr):
    c = ("31" if clr == "red" else
         "32" if clr == "green" else
         None)
    return "\033[0;{}m{}\033[0m".format(c, text) if sys.stdout.isatty() and c else text

def main():
    def positiveInt(s):
        i = int(s)
        if i < 1:
            raise argparse.ArgumentTypeError("expected positive integer")
        return i
        
    def timestamp(string):
        t = long(string)
        if t < 0:
            raise argparse.ArgumentTypeError("value is not a valid timestamp")
        return t
        
    parser = argparse.ArgumentParser(prog = "eris")
    subparsers = parser.add_subparsers()
    
    pStart = subparsers.add_parser("start")
    pStart.set_defaults(func = start)
    
    pStop = subparsers.add_parser("stop")
    pStop.set_defaults(func = stop)
    
    pStatus = subparsers.add_parser("status")
    pStatus.set_defaults(func = status)
    
    pPut = subparsers.add_parser("put")
    pPut.add_argument("-t", "--timestamp", type = timestamp, default = None, 
                      help = "Packet's timestamp (milliseconds since the epoch)")
    pPut.add_argument("-f", "--file", default = None, 
                      help = "Read from file (otherwise from stdin)")
    pPut.set_defaults(func = put)
    
    pGet = subparsers.add_parser("get")
    pGet.add_argument("-s", "--since", type = timestamp, default = 0,
                      help = "Get since given timestamp (milliseconds since the epoch)"
                      + " Retrieves all packets if not provided")
    pGet.add_argument("-l", "--limit", type = positiveInt, default = 10, 
                      help = "Limit number of packets to retrieve [default = 10]")
    pGet.set_defaults(func = get)
    
    pCount = subparsers.add_parser("packet-count")
    pCount.set_defaults(func = count)
    
    pClean = subparsers.add_parser("clean-storage")
    pClean.set_defaults(func = clean)
    
    args = parser.parse_args()
    args.func(args)
    