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
        
        # Sharding setup
        self.nshards = getattr(cfg, 'nshards', len(cfg.kvservers))
        self.nreplicas = getattr(cfg, 'nreplicas', 1)
        
        # Ensure nreplicas is consistent with the configuration
        if hasattr(cfg, 'nreplicas'):
            self.nreplicas = cfg.nreplicas
        
        # Find our server ID by looking for our position in kvservers list
        self.server_id = 0
        if hasattr(cfg, 'kvservers') and cfg.kvservers is not None:
            # Find the first None slot - that's where we'll be placed
            for i, s in enumerate(cfg.kvservers):
                if s is None:
                    self.server_id = i
                    break
            # If no None slot found, we're the last server
            else:
                self.server_id = len(cfg.kvservers)
        
        print(f"Server init: server_id={self.server_id}, nshards={self.nshards}, nreplicas={self.nreplicas}")

    def _get_shard_id(self, key: str) -> int:
        """Calculate which shard owns a key."""
        try:
            shard_id = int(key) % self.nshards
        except ValueError:
            # For non-numeric keys, use a simple hash
            shard_id = hash(key) % self.nshards
        
        print(f"Server {self.server_id}: Key {key} -> shard {shard_id}, nshards={self.nshards}")
        return shard_id

    def _is_responsible_for_shard(self, shard_id: int) -> bool:
        """Check if this server is responsible for this shard."""
        # In single-server configurations, always accept all requests
        if self.nshards == 1:
            return True
        # Server i is responsible for shard i
        return shard_id == self.server_id

    def Get(self, args: GetArgs):
        reply = GetReply(None)

        # Your code here.
        # Check if we're responsible for this shard
        shard_id = self._get_shard_id(args.key)
        if not self._is_responsible_for_shard(shard_id):
            raise KeyError(f"Server {self.server_id} not responsible for shard {shard_id}")

        self.mu.acquire()
        try:
            reply.value = self.kv_store.get(args.key, "")
        finally:
            self.mu.release()
        return reply

    def Put(self, args: PutAppendArgs):
        reply = PutAppendReply(None)

        # Your code here.
        # Check if we're responsible for this shard
        shard_id = self._get_shard_id(args.key)
        if not self._is_responsible_for_shard(shard_id):
            raise KeyError(f"Server {self.server_id} not responsible for shard {shard_id}")

        self.mu.acquire()
        try:
            op_key = (args.client_id, args.operation_id)

            if op_key in self.last_reply:
                # Duplicate operation - return the same reply
                reply.value = self.last_reply[op_key]
            else:
                # New operation - execute it
                self.kv_store[args.key] = args.value
                self.last_operation[args.client_id] = args.operation_id
                reply.value = None
                self.last_reply[op_key] = reply.value
        finally:
            self.mu.release()
        return reply

    def Append(self, args: PutAppendArgs):
        reply = PutAppendReply(None)

        # Your code here.
        # Check if we're responsible for this shard
        shard_id = self._get_shard_id(args.key)
        if not self._is_responsible_for_shard(shard_id):
            raise KeyError(f"Server {self.server_id} not responsible for shard {shard_id}")

        self.mu.acquire()
        try:
            op_key = (args.client_id, args.operation_id)

            if op_key in self.last_reply:
                # Duplicate operation - return the same reply
                reply.value = self.last_reply[op_key]
            else:
                # New operation - execute it
                current = self.kv_store.get(args.key, "")
                new_value = current + args.value
                self.kv_store[args.key] = new_value
                self.last_operation[args.client_id] = args.operation_id
                reply.value = current  # Return the OLD value (before append)
                self.last_reply[op_key] = reply.value
        finally:
            self.mu.release()
        return reply
