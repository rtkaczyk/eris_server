import bluetooth as bt
import threading, logging, select, time

import config
from connection import Connection, getConnections


log = logging.getLogger("btserver")


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
        
    def run(self):
        log.info("btserver running")
        while self.running:
            self.server_sock = None
            try:
                self.server_sock = bt.BluetoothSocket(bt.RFCOMM)
                self.server_sock.setblocking(0)
                self.server_sock.bind(("", self.channel))
                self.server_sock.listen(1)
                
                bt.advertise_service(self.server_sock, config.PNAME,
                          service_classes = [bt.SERIAL_PORT_CLASS],
                          profiles = [bt.SERIAL_PORT_PROFILE])
                
                log.info("Waiting for connections on RFCOMM channel [{}]".format(self.server_sock.getsockname()[1]))
                while self.running:
                    readable, _, _ = select.select([self.server_sock], [], [], 0.1)
                    for s in readable:
                        if s is self.server_sock:
                            client_sock, client_info = self.server_sock.accept()
                            log.info("Accepted connection from: " + str(client_info))
                            client_sock.setblocking(1)
                            client_sock.settimeout(self.timeout)
                
                            if self.running:
                                conn = Connection(client_sock, self)
                                conn.start()
            except:
                if self.running:
                    log.exception("Error while listening for connections")
                    time.sleep(1.0)
            finally:
                self.close()

    def close(self):
        try:
            if self.server_sock is not None:
                self.server_sock.close()
        except:
            pass
        
    def kill(self):
        self.running = False
        for conn in getConnections():
            try:
                conn.kill()
                conn.join()
            except:
                pass
        self.close()

