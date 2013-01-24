import threading, logging, os, time, re
from os.path import join, isdir, getmtime
from os import remove
from itertools import takewhile

import config


log = logging.getLogger("retriever")

BY_NAME  = "BY_NAME"
BY_MTIME = "BY_MTIME" 

class Retriever(threading.Thread):
    
    @staticmethod
    def castStrategy(s):
        if s in (BY_MTIME, BY_NAME):
            return s
        else:
            raise ValueError("Expected either {} or {}".format(BY_MTIME, BY_NAME))
    
    def __init__(self, storage):
        threading.Thread.__init__(self)
        self.storage = storage
        
        conf = config.getSub("retriever")
        self.interval = conf.get("interval", cast = config.nonNegativeFloat, default = 60.0)
        self.mask = re.compile(conf.get("mask", default = ".*"))
        self.strategy = conf.get("timestamp", cast = Retriever.castStrategy, default = BY_MTIME)
        self.feed = conf.get("feed", cast = config.directory, default = join(config.workDir, "feed"))
        self.batchSize = conf.get("batch", cast = config.positiveInt, default = 1)
        
        self.running = self.interval > 0.0
    
    def run(self):
        if self.running:
            log.info("retriever running")
        
        sleepPeriod = min(self.interval, 0.2)
        lastTry = 0.0
         
        while self.running:
            tau = time.time()
            if (tau - self.interval > lastTry):
                lastTry = tau
                
                files = self.getFiles()
                if len(files) > 0:
                    log.info("{} files retrieved".format(len(files)))
                else:
                    log.debug("0 files retrieved")
                    
                filesBatch = []
                for f in files:
                    if not self.running:
                        break
                    filesBatch.append(f)
                    if len(filesBatch) == self.batchSize:
                        self.put(filesBatch)
                        filesBatch = []
                    
                if len(filesBatch) > 0:
                    self.put(filesBatch)
                    
            time.sleep(sleepPeriod)
    
    def kill(self):
        self.running = False
    
    def getFiles(self):
        if not isdir(self.feed):
            log.error("{} is not a directory".format(self.feed))
            return []
        
        try:
            return [f for f in os.listdir(self.feed) if self.mask.match(f)]
        except:
            log.error("Error while listing directory", exc_info = 1)
            return []
    
    def put(self, files):
        try:
            packets = []
            for f in files:
                with open(join(self.feed, f)) as o:
                    data = o.read()
                    timestamp = self.getTime(f)
                    packets.append((timestamp, data))
                    log.debug("Inserting data from file [{}]".format(f))
                
            self.storage.put(packets)
            for f in files:
                try:
                    remove(join(self.feed, f))
                except:
                    log.error("Couldn't remove file [{}]".format(f), exc_info = 1)
        except:
            log.error("Error while retrieving data".format(f), exc_info = 1)
            
    def getTime(self, f):
        try:
            if self.strategy == BY_NAME:
                return long("".join(takewhile(lambda c: c.isdigit(), f)))
            else:
                return long(getmtime(join(self.feed, f)) * 1000)
        except:
            log.error("Couldn't evaluate packet's timestamp, using current time", exc_info = 1)
            return long(time.time() * 1000)

    