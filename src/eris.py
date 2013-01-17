import sys, os, Pyro4, psutil, logging
from datetime import datetime
 
import cli, config
from storage import Storage
from btserver import BtServer
from retriever import Retriever

log = logging.getLogger("eris")

class Eris:
    def __init__(self):
        self.output = os.path.join(config.logsDir, "output.log")
    
    def start(self):
        try:
            with open(config.statusFile, "r") as f:
                if f.read() == config.STATUS_LINE:
                    return
        except:
            pass
        try:
            with open(config.statusFile, "w") as f:
                f.write(config.STATUS_LINE)
        except IOError as e:
            print >> sys.stderr, e
            return
        
        if self.daemonize():
            return
        
        self.startTime = datetime.now()
        self.storage = Storage()
        self.btserver = BtServer(self.storage)
        self.retriever = Retriever(self.storage)
        self.btserver.start()
        self.retriever.start()
        
        daemon = Pyro4.Daemon(port = config.pyroPort)
        uri = daemon.register(self, config.PNAME)
        log.info("Eris daemon URI: [{}]".format(uri))
        self.running = True
        daemon.requestLoop(loopCondition = lambda: self.running)

        daemon.unregister(config.PNAME)
        daemon.close()
        
    def stop(self):
        log.info("Closing eris")
        self.btserver.kill()
        self.retriever.kill()
        try:
            with open(config.statusFile, "w"):
                pass
        except: 
            pass
        self.running = False
    
    def status(self):
        pid = os.getpid()
        proc = psutil.Process(pid)
        cpu = proc.get_cpu_percent()
        mem = proc.get_memory_percent()
        uptime = datetime.now() - self.startTime
        du = self.storage.size()
        return (pid, cpu, mem, uptime, du)
        
    def put(self, packets):
        self.storage.put(packets)
        
    def get(self, since = 0, to = 0, limit = None):
        connId = self.storage.get(since, to, limit)
        return self.storage.fetchall(connId)
    
    def count(self):
        return self.storage.rowcount()
        
    def ping(self):
        return config.PNAME
    
    def daemonize(self):
        try:
            pid = os.fork()
            if pid > 0:
                return True
        except OSError as e:
            sys.stderr.write("Fork #1 failed: {} ({})\n".format(e.errno, e.strerror))
            sys.exit(1)

        os.chdir("/")
        os.setsid()
        #os.umask(0)

        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError as e:
            sys.stderr.write("Fork #2 failed: {} ({})\n".format(e.errno, e.strerror))
            sys.exit(1)

        sys.stdout.flush()
        sys.stderr.flush()
        with open(self.output, "w"):
            pass
        out = file(self.output, 'a+', 1)
        os.dup2(out.fileno(), sys.stdout.fileno())
        os.dup2(out.fileno(), sys.stderr.fileno())
        return False

    @staticmethod
    def getProxy():
        uri = "PYRO:{}@localhost:{}".format(config.PNAME, config.pyroPort)
        return Pyro4.Proxy(uri) 
    
def main():
    config.configure()
    cli.main()
        
if __name__ == "__main__":
    main()
