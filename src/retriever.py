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
        
        self.running = self.interval > 0.0
    
    def run(self):
        if self.running:
            log.debug("retriever running")
        
        sleepPeriod = min(self.interval, 0.2)
        lastTry = 0.0
         
        while self.running:
            dt = time.time()
            if (dt - self.interval > lastTry):
                lastTry = dt
                
                files = self.getFiles()
                log.info("{} files retrieved".format(len(files)))
                for f in files:
                    if not self.running:
                        break
                    self.put(f)
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
    
    def put(self, f):
        try:
            with open(join(self.feed, f)) as o:
                data = o.read()
                time = self.getTime(f)
                self.storage.put([(time, data)])
                log.debug("Inserted data from file [{}]".format(f))
                try:
                    remove(join(self.feed, f))
                except:
                    log.error("Couldn't remove file [{}]".format(f), exc_info = 1)
        except:
            log.error("Error while trying to insert new data from file [{}]".format(f), exc_info = 1)
            
    def getTime(self, f):
        if self.strategy == BY_NAME:
            return "".join(takewhile(lambda c: c.isdigit(), f))
        else:
            try:
                return long(getmtime(join(self.feed, f)) * 1000)
            except:
                log.error("Couldn't get file [{}] modification date", exc_info = 1)
                return long(time.time() * 1000)
    
    