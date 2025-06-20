import random
import threading
from typing import Any, List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg

        # Your definitions here.

        self.current_server = 0

        self.client_id = nrand()
        self.seq_num = 0
        self.last_successful_op = 0

    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server[i].call("KVServer.Get", args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def get(self, key: str) -> str:
        # You will have to modify this function.

        op_id = self.seq_num
        self.seq_num += 1
        
        args = GetArgs(key)
        args.client_id = self.client_id
        args.operation_id = op_id
        args.last_operation_id = self.last_successful_op

        while True:
            try:
                reply = self.servers[self.current_server].call("KVServer.Get", args)
                self.last_successful_op = op_id
                return reply.value
            except TimeoutError:
                # Try next server
                self.current_server = (self.current_server + 1) % len(self.servers)

    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.servers[i].call("KVServer."+op, args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def put_append(self, key: str, value: str, op: str) -> str:
        # You will have to modify this function.

        op_id = self.seq_num
        self.seq_num += 1
        
        args = PutAppendArgs(key, value)
        args.client_id = self.client_id
        args.operation_id = op_id
        args.last_operation_id = self.last_successful_op

        while True:
            try:
                reply = self.servers[self.current_server].call("KVServer." + op, args)
                self.last_successful_op = op_id
                return reply.value
            except TimeoutError:
                self.current_server = (self.current_server + 1) % len(self.servers)

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
