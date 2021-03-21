# -*- coding: utf-8 -*-
import sys
import socket
import threading
import logging, logging.config
from time import time, sleep
from uuid import uuid1
import random
import string

def cerr(errmsg) :
    sys.stderr.write(errmsg + '\n')

NODE_IN_PORT = 12345
NODE_OUT_PORT = 23456
UTF8 = 'utf-8'
BUFFER = 1024
TIME_OUT = 10
LOG_SIZE = 20
BULLY_TIME = 5

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('Logger')
logger.setLevel('DEBUG')

debug = logger.debug
logerr = logging.error
logwarn = logger.warning

def gen_string_hash(N=10):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=N))

class Node(threading.Thread) :
    def __init__(self, name= 'noname') :
        super().__init__()
        self.id = uuid1()               # ipv4
        self.node_id = name
        self.name = name                # any
        self.neighbors = []             # list of NodeHandlers
        self.blocked = []               # list of NodeHandlers
        self.leader = False             #
        self.nodecount = 0              # number of neighbors
        self.running = True             # will be useful to shutdown
        self.console = Console(self)    # to command
        self.broadcast_log = []
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sckt_address = ('localhost', NODE_IN_PORT)
        self.sock.bind(sckt_address)
        self.sock.listen()
        self.leader_id = -1
        self.has_leader = False
        self.candidate = False

    def __str__(self) :
        return '<Node {} id {}>'.format(self.name, self.id)

    __repr__ = __str__

    def getId(self):
        return self.id
    
    def getName(self):
        return self.name

    def isCentral(self):
        return self.leader
    
    def getNeighbors(self):
        return self.neighbors

    def getBlocked(self):
        return self.blocked
    
    def insert_log(self, msg_id):
        self.broadcast_log.append(msg_id)
        if len(self.broadcast_log) > 2 * LOG_SIZE:
            self.broadcast_log = self.broadcast_log[-LOG_SIZE:]

    def in_log(self, msg_id):
        return msg_id in self.broadcast_log

    def removeNeighbor(self, nodehandler):
        if not nodehandler in self.neighbors:
            return
        self.neighbors.remove(nodehandler)
        self.nodecount -= 1
        debug('client {} disconected. {} clients remain'.format(
            nodehandler.getId(), self.nodecount))

    def broadcast(self, msg):
        if msg.split('$')[0] != 'broadcast':
            hash = gen_string_hash(20)
            msg = 'broadcast${}${}'.format(hash, msg)
        for node_handler in self.neighbors:
            node_handler.sendMsg(msg)

    def createConnection(self, ip, port=NODE_IN_PORT):
        try :           
            debug('args: {}, {}'.format(ip, port))
            host = socket.gethostbyname(ip) # pendiente
            other_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            addr = (host, int(port))
            other_sock.connect(addr)
        except Exception as e :
            cerr('Exception found!')
            logerr('Exception found: {}'.format(e))
            logerr(type(e))
            exit(1)
        debug('connection with {} stablished'.format(ip))
        nodehandler = NodeHandler(other_sock, -1, self)
        nodehandler.start()
        self.neighbors.append(nodehandler)
        self.nodecount += 1

        threading.Thread(target=self.bully).start()
        debug('{} clients connected'.format(self.nodecount))

    def run(self) :
        debug('Running {}'.format(self))
        debug('Looking for other nodes...')
        self.console.start()
        try :
            while self.running : # seeker
                conn, addr = self.sock.accept()
                debug('connection accepted from {}. '.format(addr))
                nodehandler = NodeHandler(conn, -1, self)
                nodehandler.start()
                self.neighbors.append(nodehandler)
                self.nodecount += 1
                debug('{} clients connected'.format(self.nodecount))
        except KeyboardInterrupt :
            print('Shutting down from Node')
        except Exception as e:
            cerr('Error on Node.run')
            cerr('exiting with status 1')
            logerr('Exception found: {}'.format(e))
            print(type(e))
            exit(1)

    def bully(self):
        msg = 'election${}'.format(self.node_id)
        self.broadcast(msg)
        self.candidate = True
        sleep(BULLY_TIME)
        if self.candidate:
           self.victory() 

    def action_higher(self):
        self.bully()

    def victory(self):
        msg = 'victory${}'.format(self.node_id)
        self.set_leader(self.node_id)
        self.broadcast(msg)
        print('victory from node {}'.format(self.node_id))

    def action_lower(self): # this is a thread
        self.candidate = False
        sleep(BULLY_TIME)
        if not self.has_leader:
            self.bully()

    def set_leader(self, id):
        self.has_leader = True
        self.leader_id = id


class NodeHandler(threading.Thread):
    def __init__(self, node_socket, node_id, node):
        super().__init__()
        self.running = True
        self.daemon = True
        self.node_sock = node_socket
        self.node_id = node_id
        self.node = node
        self.is_alive = True
        msg = "identify${}".format(self.node.node_id)
        sleep(0.5)
        self.sendMsg(msg)

    def getId(self):
        return self.node_id
    
    def sendMsg(self, msg):
        try:
            byte_msg = bytes(msg, UTF8)
            self.node_sock.sendall(byte_msg)
        except:
            cerr('Error on sendMessage')
            exit(1)

    def kill_node(self):
        print(self.node_id, self.node.leader_id)
        if self.node.leader_id in self.node_id:
            threading.Thread(target=self.node.bully).start()
        self.node.removeNeighbor(self)
        self.running = False

    def ping(self):
        cnt = 0
        while cnt < 3:
            self.is_alive = False
            self.sendMsg('ping')
            sleep(TIME_OUT)
            if self.is_alive: 
                cnt = 0
            else:
                cnt += 1
        debug('connection closed. Stopping.')
        self.kill_node()

    def run(self) :
        debug('Running node handler')
        heartbit = threading.Thread(target=self.ping)
        heartbit.start()
        while self.running:
            data = self.node_sock.recv(BUFFER)
            if data != b'':
                input_data = data.decode(UTF8)
                arr = input_data.split('$')
                print(arr)
                if 'identify' in arr[0]:
                    self.node_id = arr[1]
                elif 'ping' in arr[0]:
                    self.sendMsg('pong')
                elif 'pong' in arr[0]:
                    self.is_alive = True
                elif 'broadcast' in arr[0]:
                    if not self.node.in_log(arr[1]):
                        self.node.insert_log(arr[1])
                        self.node.broadcast(input_data)
                        if 'victory' in arr[2]:
                            print('victory for {}'.format(arr[3]))
                            self.node.set_leader(arr[3])
                        elif 'election' in arr[2]:
                            election_id = arr[3]
                            if election_id < self.node.node_id:
                                threading.Thread(target=self.node.action_higher).start()
                            elif election_id > self.node.node_id:
                                threading.Thread(target=self.node.action_lower).start()
                        # decidir que se hace con la data


                # print('received: {}'.format(input_data))      
            else:
                print('interrupt')
                self.kill_node()   

            # process data and send response
            # here comes bully algorithm
                

class Console(threading.Thread) :
    """We are gonna control the node from here"""
    def __init__(self, node) :
        super().__init__()
        self.daemon = True
        self.node = node
        debug('Console initiated. Type something')
    
    def process(self, cmd_str : str) :
        cmd_list = cmd_str.split(' ')
        cmd = []
        [cmd.append(_) for _ in cmd_list if _ != '']

        if cmd[0] == 'node':
            print(self.node)
        elif cmd[0] == 'id':
            print(self.node.node_id)
        elif cmd[0] == 'neighbors':
            print(self.node.neighbors)
        elif cmd[0] == 'connect':
            print(*cmd[1:])
            self.node.createConnection(*cmd[1:])
        elif cmd[0] == 'shutdown':
            self.node.running = False
            # kill subprocess
        elif 'broadcast' in cmd[0]:
            # self.node.broadcast('')
            pass
        else :
            print("Command '{}' unknown.".format(cmd))

    def run(self) :
        while self.node.running :
            cmd = input('prompt> ')
            
            if cmd == '' :
                print("(*≧∀≦*)")
                continue

            self.process(cmd)

if __name__ == '__main__' :
    name = gen_string_hash()
    p = Node(name)
    p.start()
