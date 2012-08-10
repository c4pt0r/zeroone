#ZeroOne Service broker
#written by c4pt0r, modefied from mdbroker.c from zeromq examples

#encoding=utf-8

import sys
import zmq
from binascii import hexlify
import time

import zo_api
import logging

class Service(object):
    name = None
    requests = None # list of waiting requests
    waiting = None # list of waiting service worker

    def __init__(self, name):
        self.name = name
        self.requests = []
        self.waiting = []

class Worker(object):
    identity = None
    address = None
    service = None

    def __init__(self, id, address):
        self.identity = id
        self.address = address


class ZOBroker(object):
    ctx = None
    socket = None
    poller = None

    services = None
    workers = None
    waiting = None

    verbose = False


    def __init__(self, verbose=False):
        self.verbose = verbose

        self.services = {}
        self.workers = {}

        self.waiting = []
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.ROUTER)
        self.poller = zmq.Poller()

        self.poller.register(self.socket, zmq.POLLIN)

        logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                level=logging.INFO)


    def run(self):
        while True:
            try:
                socks = self.poller.poll()
            except:
                break

            if socks:
                msg = self.socket.recv_multipart()
                if self.verbose:
                    logging.info("I: received message.")

                sender = msg.pop(0)
                empty = msg.pop(0)
                assert empty == ''

                header = msg.pop(0)
                if header == zo_api.FLG_CLIENT:
                    self.handle_client(sender, msg)
                if header == zo_api.FLG_SERVICE:
                    self.handle_worker(sender, msg)


    def handle_client(self, sender, msg):
        assert len(msg) >= 2 # service name + body
        service = msg.pop(0)

        msg = [sender,''] + msg
        if service == 'CHECK':
            self.check(service)
        else:
            self.dispatch(self.require_service(service), msg)

    def handle_worker(self, sender, msg):
        assert len(msg) >= 1 # at least, command
        command = msg.pop(0)
        worker_already = hexlify(sender) in self.workers

        worker = self.require_worker(sender)
        
        if command == zo_api.M_READY:
            service = msg.pop(0)
            # if this worker already exists, kill it and replace
            if worker_already:
                self.delete_worker(worker, True)
            else:
                worker.service = self.require_service(service)
                self.worker_waiting(worker)

        elif command == zo_api.M_REPLY:
            if worker_already:
                client = msg.pop(0)
                empty = msg.pop(0)
                msg = [client, '', zo_api.FLG_CLIENT, worker.service.name] + msg
                self.socket.send_multipart(msg)
                self.worker_waiting(worker)
    
    def worker_waiting(self ,worker):
        self.waiting.append(worker)
        worker.service.waiting.append(worker)
        self.dispatch(worker.service, None)

    def require_worker(self, address):
        identity = hexlify(address)
        worker = self.workers.get(identity)
        if worker is None:
            worker = Worker(identity, address)
            self.workers[identity] = worker
            if self.verbose:
                logging.info("registering new service worker: %s", identity)
        return worker

    def require_service(self, name):
        service = self.services.get(name)
        if service is None:
            service = Service(name)
            self.services[name] = service
        return service

    def dispatch(self, service, msg):
        if msg is not None:
            service.requests.append(msg)
        while service.waiting and service.requests:
            msg = service.requests.pop(0)
            worker = service.waiting.pop(0)
            self.waiting.remove(worker)
            if not isinstance(msg, list):
                msg = [msg]
            #self.send_to_worker(worker, zo_api.M_REQUEST, None, msg)
            msg = [worker.address, '', zo_api.FLG_SERVICE , zo_api.M_REQUEST] + msg
            self.socket.send_multipart(msg)

    def bind(self, endpoint):
        self.socket.bind(endpoint)
        logging.info("binding at %s", endpoint)

def main():
    broker = ZOBroker(True)
    broker.bind("tcp://*:%s" % sys.argv[1])
    broker.run()

if __name__ == '__main__':
    main()