import logging
import threading
from typing import Tuple, Any

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key, value):
        self.key = key
        self.value = value

        self.client_id = None
        self.operation_id = None
        self.last_operation_id = None

class PutAppendReply:
    # Add definitions here if needed

    def __init__(self, value):
        self.value = value

class GetArgs:
    # Add definitions here if needed

    def __init__(self, key):
        self.key = key

        self.client_id = None
        self.operation_id = None
        self.last_operation_id = None
class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg

        # Your definitions here.
        self.kv_store = {}

        self.last_operation = {}

        self.last_reply = {}  

    def Get(self, args: GetArgs):
        reply = GetReply(None)

        # Your code here.

        self.mu.acquire()
        try:
            reply.value = self.kv_store.get(args.key, "")
        finally:
            self.mu.release()
        return reply

    def Put(self, args: PutAppendArgs):
        reply = PutAppendReply(None)

        # Your code here.

        self.mu.acquire()
        try:
            op_key = (args.client_id, args.operation_id)

            if op_key in self.last_reply:
                reply.value = self.last_reply[op_key]
            else:
                # New operation - execute it
                self.kv_store[args.key] = args.value
                self.last_operation[args.client_id] = args.operation_id
                reply.value = None
                self.last_reply[op_key] = reply.value
            # Clean up old state based on last_operation_id
            if args.last_operation_id > 0:
                pass
        finally:
            self.mu.release()
        return reply

    def Append(self, args: PutAppendArgs):
        reply = PutAppendReply(None)

        # Your code here.

        self.mu.acquire()
        try:
            op_key = (args.client_id, args.operation_id)

            if op_key in self.last_reply:
                reply.value = self.last_reply[op_key]
            else:
                # New operation - execute it
                current = self.kv_store.get(args.key, "")
                self.kv_store[args.key] = current + args.value
                self.last_operation[args.client_id] = args.operation_id
                reply.value = current  # Return the OLD value
                self.last_reply[op_key] = reply.value
            # Clean up old state based on last_operation_id
            if args.last_operation_id > 0:
                pass
        finally:
            self.mu.release()
        return reply
