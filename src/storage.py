import os, sys, sqlite3, logging, time
from os import path

import config

log = logging.getLogger("storage")
VAC_BATCH = 1024

class StorageTimeout(Exception): pass

class Storage:
    def __init__(self):
        self.writeLock = False
        self.readLock = False
        self.deleteLock = False
        self.getsPending = 0
        
        conf = config.getSub("storage")
        self.capacity = conf.get("capacity", cast = config.positiveInt, default = 2048)
        self.vacPercent = conf.get("vacuum_percent", cast = config.positivePercent, default = 20.0) / 100
        self.delSent = conf.get("delete_sent", cast = config.boolean, default = False)
        self.timeout = conf.get("timeout", cast = config.positiveFloat, default = 10.0)
        
        try:
            conn = sqlite3.connect(self.dbFile())
            with conn:
                conn.execute("CREATE TABLE IF NOT EXISTS packets (timestamp UNSIGNED INT8, data BLOB)")
            self.vaccum()
        except Exception:
            log.critical("Failed to initialize database", exc_info = 1)
            sys.exit(1)
        
        self.connections = {}
        log.info("Database initialized")
        
    def vaccum(self):
        if self.size() >= self.capacity:
            log.debug("Size: {}, Capacity: {}".format(self.size(), self.capacity))
            try:
                self.waitForLocks("rwd")
                self.setLocks("rwd")
                
                cutoff = 0
                count = self.rowcount()
                
                with sqlite3.connect(self.dbFile()) as conn:
                    log.info("Vacuuming storage. About to delete {}% packets".format(self.vacPercent * 100))
                    conn = sqlite3.connect(self.dbFile())
                    c = conn.cursor()
                    c.execute("SELECT timestamp FROM packets")
                    if count <= 0:
                        log.error("Storage size is over limit, but there are no packets. Verify configuration")
                        return
                    found = 0
                    while True:
                        ts = c.fetchmany(VAC_BATCH)
                        if len(ts) == 0:
                            break
                        if float(found + len(ts)) / count >= self.vacPercent:
                            x = int(count * self.vacPercent - found)
                            if x < 0:
                                x = 0
                            if x >= len(ts):
                                x = -1
                            cutoff = ts[x][0]
                            break
                        else:
                            found += len(ts)
                            cutoff = ts[-1][0]
                    c.execute("DELETE FROM packets WHERE timestamp <= ?", (long(cutoff), ))
                    log.info("Deleting packets with timestamp <= {}. In total {} out of {}".format(cutoff, c.rowcount, count))
                    conn.commit()
                
                with sqlite3.connect(self.dbFile()) as conn:
                    conn.execute("VACUUM")
                    conn.commit()

            except:
                log.error("Vacuuming database failed", exc_info = 1)
            finally:
                self.unsetLocks("rwd")
                log.debug("Size: {}, Capacity: {}".format(self.size(), self.capacity))
    
    def size(self):
        try:
            return path.getsize(self.dbFile()) / 1024
        except:
            return 0
        
    def put(self, packets):
        log.info("Inserting {} packets".format(len(packets)))
        try:
            self.waitForLocks("w")
            self.setLocks("wd")
            
            conn = sqlite3.connect(self.dbFile())
            with conn:
                for p in packets:
                    packet = (long(time.time() * 1000) if p[0] is None else long(p[0]), buffer(p[1]))
                    conn.execute("INSERT INTO packets VALUES(?, ?)", packet)
                conn.commit()
            self.unsetLocks("d")
            self.vaccum()
        except Exception:
            log.error("Failed to insert packets into db", exc_info = 1)
        finally:
            self.unsetLocks("wd")
            
    def get(self, since = 0, to = 0, limit = 0):
        try:
            self.waitForLocks("r")
            self.setLocks("d")
            self.addPending()
            if self.delSent:
                self.setLocks("w")
            
            to = long(to) if to > 0 else long(2 ** 63 - 1)
            since = long(since)
            
            log.info("Retrieving packets (since={}, to={}, limit={})".format(since, to, limit))
            
            conn = sqlite3.connect(self.dbFile())
            cursor = conn.cursor()
            
            if limit > 0:
                cursor.execute("SELECT count(*) FROM packets WHERE timestamp > ? AND timestamp < ? LIMIT ?", 
                          (since, to, limit))
                count = cursor.fetchone()[0]
                cursor.execute("SELECT timestamp, data FROM packets WHERE timestamp > ? AND timestamp < ?" + 
                               "ORDER BY timestamp DESC LIMIT ?", (since, to, limit))
            else:
                cursor.execute("SELECT count(*) FROM packets WHERE timestamp > ? AND timestamp < ?", 
                          (since, to))
                count = cursor.fetchone()[0]
                cursor.execute("SELECT timestamp, data FROM packets WHERE timestamp > ? AND timestamp < ? " + 
                               "ORDER BY timestamp DESC", (since, to))
            connId = self.getConnId()
            self.connections[connId] = (conn, cursor)
            return connId, count
            
        except Exception:
            log.error("Failed to retrieve packets from db", exc_info = 1)
            self.unsetLocks("d")
            
            return None, 0
        
    def closeConn(self, connId):
        (conn, _) = self.connections.get(connId, (None, None))
        if conn is None:
            return
        try:
            del self.connections[connId]
            if not self.delSent:
                self.subPending
            conn.close()
        except:
            log.warn("Error closing connection to db", exc_info = 1)
        finally:
            if self.getsPending == 0:
                self.unsetLocks("d")
            if not self.delSent:
                self.unsetLocks("w")
        
        
    def rowcount(self, since = 0, to = 0, limit = 0):
        try:
            conn = sqlite3.connect(self.dbFile())
            c = conn.cursor()
            if since == 0 and to == 0 and limit is None:
                c.execute("SELECT count(*) FROM packets")
            else:
                to = long(to) if to > 0 else long(2 ** 63 - 1)
                since = long(since)
                c.execute("SELECT count(*) FROM packets WHERE timestamp > ? AND timestamp < ? LIMIT ?", 
                          (since, to, limit))
            size = c.fetchone()[0]
            conn.close()
            return size
        except:
            log.error("Failed to retrieve packet count", exc_info = 1)
            return 0

        
    def fetch(self, connId, n = 1):
        (conn, cursor) = self.connections.get(connId, (None, None))
        if conn is None:
            return []

        try:
            result = cursor.fetchmany(size = n)
            if result is None or len(result) == 0:
                self.closeConn(connId)
                return []
            else:
                log.info("Fetched {} packets".format(len(result)))
                return self.debuffer(result)
        except Exception:
            log.error("Failed to fetch packets from db", exc_info = 1)
            self.closeConn(connId)
            return []
    
    def fetchall(self, connId):
        (conn, cursor) = self.connections.get(connId, (None, None))
        if conn is None:
            return []

        try:
            result = cursor.fetchall()
            conn.close()
            del self.connections[connId]
            log.info("Fetched {} packets".format(len(result)))
            return self.debuffer(result)
        except Exception:
            log.error("Failed to fetch packets from db", exc_info = 1)
            self.closeConn(connId)
            return []
    
    def deleteSent(self, since, to, limit):
        if self.delSent:
            log.info("Deleting sent packets")
            try:
                self.waitForLocks("d")
                self.setLocks("rwd")
                with sqlite3.connect(self.dbFile()) as conn:
                    conn = sqlite3.connect(self.dbFile())
                    c = conn.cursor()
                    c.execute("DELETE FROM packets WHERE timestamp <= ?", (since, to, limit))
                    conn.commit()
            except:
                log.exception("Could not delete sent packets")
            finally:
                self.unsetLocks("d")
                self.subPending
                if self.getsPending == 0:
                    self.unsetLocks("rw")
        
    def release(self, connId):
        if connId in self.connections:
            (conn, _) = self.connections.get(connId, (None, None))
            if conn is not None:
                try:
                    conn.close()
                except:
                    pass
            del self.connections[connId]
            
    def addPending(self):
        self.getsPending += 1
        
    def subPending(self):
        if self.getsPending < 0:
            self.getsPending = 0
            log.error("Pending gets dropped below zero")
            
    def waitForLocks(self, mode):
        t0 = time.time()
        tau = 0.0
        dt = 0.05
        cond = lambda: (
            ("r" in mode and self.readLock) or  
            ("w" in mode and self.writeLock) or 
            ("d" in mode and self.deleteLock)
        )
#        if "r" in mode and "w" in mode:
#            cond = lambda: self.writeLock or self.readLock
#        elif "r" in mode:
#            cond = lambda: self.readLock
#        elif "w" in mode:
#            cond = lambda: self.writeLock
#        else:  
        while cond():
            time.sleep(dt)
            tau += dt
            if tau >= 1.0:
                log.info("Waiting for database")
                tau = 0.0
            if time.time() - t0 > self.timeout:
                raise StorageTimeout("Database request timed out")
    
    def setLocks(self, mode):
        if "r" in mode:
            self.readLock = True
        if "w" in mode:
            self.writeLock = True
        if "d" in mode:
            self.deleteLock = True
            
    def unsetLocks(self, mode):
        if "r" in mode:
            self.readLock = False
        if "w" in mode:
            self.writeLock = False
        if "d" in mode:
            self.deleteLock = False
            
    def debuffer(self, result):
        return [(t, str(d)) for t, d in result]
    
    @staticmethod    
    def dbFile():
        return os.path.join(config.workDir, "eris.db")
    
    def getConnId(self):
        connId = 0
        while True:
            connId += 1
            if connId >= 2 ** 31:
                connId = 1
            yield connId