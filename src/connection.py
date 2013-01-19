import threading, logging
import bt_pb2
from config import genConnId

log = logging.getLogger("btserver")


connections = {}
connectionId = genConnId()

def getConnections():
    return connections.values()


class InvalidRequest(Exception): pass
class InternalError(Exception): pass


class Connection(threading.Thread):
    def __init__(self, sock, btserver):
        threading.Thread.__init__(self)
        
        self.connId = connectionId.next()
        connections[self.connId] = self
        
        self.sock = sock
        self.storage = btserver.storage
        self.delSent = btserver.delSent
        self.batch = btserver.batch
        self.running = True
        
    def run(self):
        try:
            self.processRequest()
            
        except InternalError as e:
            log.exception("Internal Error during bluetooth comms")
            self.sendError(bt_pb2.Error.INTERNAL_ERROR, e.message)
        
        except InvalidRequest as e:
            log.exception("Invalid bluetooth request")
            self.sendError(bt_pb2.Error.INVALID_REQUEST, e.message)
            
        except:
            if self.running:
                log.exception("Unexpected error during bluetooth comms")
        
        finally:
            log.debug("Closing client socket")
            self.kill()
    
    def processRequest(self):
        timerange = (2 ** 63, 0)
        def updateTimerange(frm, to):
            f = min(timerange[0], frm)
            t = max(timerange[1], to)
            return (f, t)
        
        dbConnId = 0
        try:
            n = self.readLen()
            log.debug("Request is {} bytes long".format(n))
            serialized = self.sock.recv(n) if n > 0 else ""
            request = bt_pb2.Request()
            try:
                request.ParseFromString(serialized)
            except:
                raise InvalidRequest("Couldn't parse request")
            
            dbConnId, packetCount = self.storage.get(request.frm, request.to, request.limit)
            if dbConnId is None:
                raise InternalError("Database error")
            
            batch = request.batch if request.batch > 0 else self.batch
            full = request.full
            initial = True
            while self.running:
                response = bt_pb2.Response()
                packets = self.storage.fetch(dbConnId, n = batch)
                if len(packets) > 0:
                    timerange = updateTimerange(packets[-1][0], packets[0][0])
                    if not full:
                        response.frm = packets[-1][0]
                        response.to = packets[0][0]
                    if initial:
                        response.noPackets = packetCount
                        initial = False
                        
                for (t, data) in packets:
                    packet = response.packets.add()
                    if full:
                        packet.timestamp = t
                    packet.data = data
                    
                serialized = response.SerializeToString()
                self.writeLen(len(serialized))
                log.debug("Response is {} bytes long".format(len(serialized)))
                self.sock.send(serialized)
                if len(packets) == 0:
                    break
            
            n = self.readLen()
            if n == packetCount:
                self.writeLen(1)
                log.info("{} packets sent".format(n))
                if self.delSent and packetCount > 0:
                    self.storage.delete(*timerange)
            else:
                log.warn("Client did not respond with correct number of packets. Expected: {}, actual: {}".
                         format(packetCount, n))
                self.writeLen(0)
            
            
            log.info("Finishing communications")
            self.shutdownSock()
        finally:
            self.storage.closeConn(dbConnId)

    def readLen(self):
        ret = 0
        i = 0
        byte = 0
        while True:
            try:
                byte = ord(self.sock.recv(1)[0])
            except:
                raise InvalidRequest("Request length unavailable")
            ret += (byte & 0x7F) << (i * 7)
            i += 1
            if byte & 0x80 == 0:
                break
            if i > 5:
                raise InvalidRequest("Request too long")
        return ret
        
    def writeLen(self, n):
        if n > 2 ** 31 - 1:
            raise InvalidRequest("Response too long")
        i = 0
        while True:
            byte = (n >> (7 * i)) & 0x7F
            msb = 0x80 if (n >> (7 * (i + 1))) > 0 else 0x0
            i += 1
            byte |= msb
            self.sock.send(chr(byte))
            if msb == 0:
                break

    def sendError(self, code, desc = ""):
        try:
            log.debug("Sending error message")
            response = bt_pb2.Response()
            response.error.code = code
            if desc:
                response.error.description = desc
            serialized = response.SerializeToString()
            self.writeLen(len(serialized))
            self.sock.send(serialized)
        except:
            log.exception("Couldn't send error message")
        finally:
            self.shutdownSock()
        
    def shutdownSock(self):
        try:
            self.sock.recv(1)
        except:
            pass
        
    def kill(self):
        self.running = False
        try:
            del connections[self.connId]
        except:
            pass
        try:
            self.sock.close()
        except:
            pass