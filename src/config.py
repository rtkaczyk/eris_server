import os, yaml, sys, logging.config, Pyro4, traceback


log = logging.getLogger("eris")

class Config:
    def __init__(self, dic):
        self.dic = dic if isinstance(dic, dict) else {}
    
    def get(self, name, cast = lambda o: o, default = None):
        try:
            val = self.dic.get(name)
            if val is None:
                log.debug("Returning default value for " + name)
                return default
            return cast(val)
        except:
            log.warn("Invalid value for {}: [{}]".format(name, val), exc_info = 1)
            return default
    
    def getSub(self, name):
        dic = self.dic.get(name)
        return Config(dic)



baseDir = None
confDir = None
logsDir = None
workDir = None

mainConfig = Config({})

pyroPort = None
statusFile = None
PNAME = "eris"
STATUS_LINE = "WORKING"



def configure():
    global baseDir, confDir, logsDir, workDir
    baseDir = os.getenv("ERIS_BASEDIR", None)
    if baseDir == None:
        print >> sys.stderr, "ERIS_BASEDIR environment variable is not set"
        sys.exit(1)
    confDir = os.path.join(baseDir, "conf")
    logsDir = os.path.join(baseDir, "logs")
    workDir = os.path.join(baseDir, "work")
    
    confLogging()
    confMain()
    confBasic()
    confPyro()
    
def confLogging():
    try:
        with open(os.path.join(confDir, "logging.yaml")) as f:
            d = yaml.load(f)
            for h in d.get("handlers", {}).itervalues():
                filename = h.get("filename")
                if filename:
                    h["filename"] = filename.replace("${LOGS_DIR}", logsDir)
            logging.config.dictConfig(d)
    except Exception:
        print >> sys.stderr, "Error: Unable to configure logging"
        print >> sys.stderr, traceback.print_exc()
        sys.exit(1)
        
def confMain():
    global mainConfig
    try:
        with open(os.path.join(confDir, "eris.yaml")) as f:
            d = yaml.load(f)
            mainConfig = Config(d)
    except Exception as e:
        log.warn("Main configuration not found: " + str(e))
        
def confBasic():
    global statusFile
    statusFile = os.path.join(workDir, "status")
    
def confPyro():
    global pyroPort
    conf = getSub("pyro")
    pyroPort = conf.get("port", cast = port, default = 7017)
    Pyro4.config.COMMTIMEOUT = conf.get("timeout", cast = positiveFloat, default = 4.0)
    Pyro4.config.HMAC_KEY = PNAME
    Pyro4.config.SOCK_REUSE = True


def get(name, cast = lambda o: o, default = None):
    return mainConfig.get(name, cast, default)
    
def getSub(name):
    return mainConfig.getSub(name)



def port(s):
    ret = int(s)
    if ret not in xrange(1, 2**16):
        raise ValueError("Invalid port value")
    return ret
    
def positiveInt(s):
    i = int(s)
    if i < 1:
        raise ValueError("Expected positive integer")
    return i

def positiveFloat(s):
    f = float(s)
    if not f > 0.0:
        raise ValueError("Expected positive float")
    return f

def nonNegativeFloat(s):
    f = float(s)
    if f < 0.0:
        raise ValueError("Expected non-negative float")
    return f

def positivePercent(s):
    f = float(s)
    if not 100.0 >= f > 0.0:
        raise ValueError("Expected a number in range (0.0, 100.0]")
    return f

def channel(s):
    c = int(s)
    if c not in range(1, 31):
        raise ValueError("RFCOMM channel should be in range [1, 30]")
    return c

def directory(s):
    log.debug("directory: [{}]".format(s))
    s = s.replace("${WORK_DIR}", workDir)
    s = s.replace("${LOGS_DIR}", logsDir)
    s = s.replace("${CONF_DIR}", confDir)
    log.debug("directory: [{}]".format(s))
    return s

