import bluetooth as bt
import threading, logging, select

import config, bt_pb2


log = logging.getLogger("btserver")


class InvalidRequest(Exception): pass
class InternalError(Exception): pass

class BtServer(threading.Thread):
    def __init__(self, storage):
        threading.Thread.__init__(self)
        self.storage = storage
        
        conf = config.getSub("bluetooth")
        self.channel = conf.get("rfcomm_channel", cast = config.channel, default = 2)
        self.batch = conf.get("batch_size", cast = config.positiveInt, default = 10)
        self.timeout = conf.get("timeout", cast = config.positiveFloat, default = 10.0)
        self.delSent = conf.get("delete_sent", cast = config.boolean, default = False)
        
        self.running = True
        
        self.server_sock = None
        self.client_sock = None
        
    def run(self):
        while self.running:
            log.debug("btserver running")
            self.server_sock = None
            self.client_sock = None
            try:
                self.server_sock = bt.BluetoothSocket(bt.RFCOMM)
                self.server_sock.setblocking(0)
                self.server_sock.bind(("", self.channel))
                self.server_sock.listen(1)
                
                bt.advertise_service(self.server_sock, config.PNAME,
                          service_classes = [bt.SERIAL_PORT_CLASS],
                          profiles = [bt.SERIAL_PORT_PROFILE])
                
                log.info("Waiting for connection on RFCOMM channel [{}]".format(self.server_sock.getsockname()[1]))
                while self.running and self.client_sock is None:
                    readable, _, _ = select.select([self.server_sock], [], [], 0.1)
                    for s in readable:
                        if s is self.server_sock:
                            self.client_sock, client_info = self.server_sock.accept()
                            log.info("Accepted connection from: " + str(client_info))
                
                self.client_sock.setblocking(1)
                self.client_sock.settimeout(self.timeout)
                if self.running:
                    self.processRequest()
                
            except InternalError as e:
                log.error("Internal Error during bluetooth comms", exc_info = 1)
                try:
                    self.sendError(self.client_sock, bt_pb2.Error.Code.INTERNAL_ERROR, e.message)
                    self.shutdownSock(self.client_sock)
                except:
                    log.error("Couldn't send error message", exc_info = 1)
            
            except InvalidRequest as e:
                log.error("Invalid bluetooth request", exc_info = 1)
                try:
                    self.sendError(self.client_sock, bt_pb2.Error.Code.INVALID_REQUEST, e.message)
                    self.shutdownSock(self.client_sock)
                except:
                    log.error("Couldn't send error message", exc_info = 1)
                
            except:
                if self.running:
                    log.error("Unexpected error during bluetooth comms", exc_info = 1)
            
            finally:
                log.debug("Closing sockets")
                self.closeSockets()
                
    def processRequest(self):
        timerange = (2 ** 63, 0)
        def updateTimerange(frm, to):
            f = min(timerange[0], frm)
            t = max(timerange[1], to)
            return (f, t)
        
        try:
            n = self.readLen(self.client_sock)
            log.debug("Request is {} bytes long".format(n))
            serialized = self.client_sock.recv(n) if n > 0 else ""
            request = bt_pb2.Request()
            try:
                request.ParseFromString(serialized)
            except:
                raise InvalidRequest("Couldn't parse request")
            
            connId, packetCount = self.storage.get(request.frm, request.to, request.limit)
            if connId is None:
                raise InternalError("Database error")
            
            batch = request.batch if request.batch > 0 else self.batch
            full = request.full
            initial = True
            while self.running:
                response = bt_pb2.Response()
                packets = self.storage.fetch(connId, n = batch)
                if len(packets) > 0:
                    timerange = updateTimerange(packets[0][0], packets[-1][0])
                    if not full:
                        response.to = packets[0][0]
                        response.frm = packets[-1][0]
                    if initial:
                        response.noPackets = packetCount
                        initial = False
                        
                for (t, data) in packets:
                    packet = response.packets.add()
                    if full:
                        packet.timestamp = t
                    packet.data = data
                    
                serialized = response.SerializeToString()
                self.writeLen(self.client_sock, len(serialized))
                log.debug("Response is {} bytes long".format(len(serialized)))
                self.client_sock.send(serialized)
                if len(packets) == 0:
                    break
            
            n = self.readLen(self.client_sock)
            if n == packetCount:
                self.writeLen(self.client_sock, 1)
                log.info("{} packets sent".format(n))
                if self.delSent:
                    self.storage.delete(*timerange)
            else:
                log.warn("Client did not respond with correct number of packets. Expected: {}, actual: {}".
                         format(packetCount, n))
                self.writeLen(self.client_sock, 0)
            
            
            log.info("Finishing communications")
            self.shutdownSock(self.client_sock)
        finally:
            self.storage.closeConn(connId)

    def readLen(self, sock):
        ret = 0
        i = 0
        byte = 0
        while True:
            try:
                byte = ord(sock.recv(1)[0])
            except:
                raise InvalidRequest("Request length unavailable")
            ret += (byte & 0x7F) << (i * 7)
            i += 1
            if byte & 0x80 == 0:
                break
            if i > 5:
                raise InvalidRequest("Request too long")
        return ret
        
    def writeLen(self, sock, n):
        if n > 2 ** 31 - 1:
            raise InvalidRequest("Response too long")
        i = 0
        while True:
            byte = (n >> (7 * i)) & 0x7F
            msb = 0x80 if (n >> (7 * (i + 1))) > 0 else 0x0
            i += 1
            byte |= msb
            sock.send(chr(byte))
            if msb == 0:
                break

    def sendError(self, sock, code, desc = ""):
        response = bt_pb2.Response
        error = bt_pb2.Error()
        error.code = code
        if desc:
            error.description = desc
        response.error = error
        serialized = response.SerializeToString()
        self.writeLen(sock, len(serialized))
        sock.send(serialized)
        
    def shutdownSock(self, sock):
        try:
            sock.recv(1)
        except:
            pass
        sock.shutdown(1)
        sock.close()
        
    def closeSockets(self):
        try:
            if self.server_sock is not None:
                self.server_sock.close()
        except:
            pass
        try:
            if self.client_sock is not None:
                self.client_sock.close()
        except:
            pass
        
    def kill(self):
        self.running = False
        self.closeSockets()

