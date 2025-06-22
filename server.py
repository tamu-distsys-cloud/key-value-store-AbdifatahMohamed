import logging
import threading
from typing import Tuple, Any, List

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Global counter for server IDs
_server_id_counter = 0
_server_id_lock = threading.Lock()

def reset_server_id_counter():
    global _server_id_counter
    with _server_id_lock:
        _server_id_counter = 0

def get_next_server_id():
    global _server_id_counter
    with _server_id_lock:
        server_id = _server_id_counter
        _server_id_counter += 1
        return server_id

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
    def __init__(self, cfg, server_id=None):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.kv_store = {}
        self.processed_ops = set()
        self.last_reply = {}

        # Server ID and configuration
        # Use a deterministic approach: count existing servers to get the next ID
        # This ensures server IDs match the array indices used by the client
        existing_servers = [s for s in cfg.kvservers if s is not None]
        self.server_id = len(existing_servers)
        self.nshards = getattr(cfg, 'nshards', len(cfg.kvservers))
        self.nreplicas = getattr(cfg, 'nreplicas', 1)
        
        print(f"Server init: server_id={self.server_id}, nshards={self.nshards}")

    def _get_shard_id(self, key: str) -> int:
        """Calculate shard ID for a key."""
        try:
            shard_id = int(key) % self.nshards
        except ValueError:
            shard_id = hash(key) % self.nshards
        return shard_id

    def _get_replica_group(self, shard_id: int) -> List[int]:
        """Compute the replica group for a shard using the *current* cfg.nreplicas value."""
        total_servers = len(self.cfg.kvservers)
        nreplicas = getattr(self.cfg, 'nreplicas', 1)
        replica_group = [(shard_id + i) % total_servers for i in range(nreplicas)]
        return replica_group

    def _is_primary_for_shard(self, shard_id: int) -> bool:
        """Check if this server is the primary for the given shard."""
        replica_group = self._get_replica_group(shard_id)
        return self.server_id == replica_group[0]

    def _is_replica_for_shard(self, shard_id: int) -> bool:
        """Check if this server is a replica for the given shard."""
        replica_group = self._get_replica_group(shard_id)
        return self.server_id in replica_group

    def Get(self, args: GetArgs):
        reply = GetReply("")
        shard_id = self._get_shard_id(args.key)
        
        # Check if this server is responsible for this shard
        if not self._is_replica_for_shard(shard_id):
            raise KeyError(f"Shard {shard_id} not owned by server {self.server_id}")
        
        # If not primary, forward the read to the primary server for up-to-date value.
        if not self._is_primary_for_shard(shard_id):
            primary_id = self._get_replica_group(shard_id)[0]
            primary_server = self.cfg.kvservers[primary_id]
            return primary_server.Get(args)
        
        # Handle duplicate operations safely under lock
        op_id = (args.client_id, args.operation_id)
        with self.mu:
            if op_id in self.processed_ops:
                return GetReply(self.last_reply[op_id])

            # First execution: apply read and remember reply
            val = self.kv_store.get(args.key, "")
            self.processed_ops.add(op_id)
            self.last_reply[op_id] = val

        reply.value = val
        return reply

    def Put(self, args: PutAppendArgs):
        reply = PutAppendReply("")
        shard_id = self._get_shard_id(args.key)
        
        # Check if this server is responsible for this shard
        if not self._is_replica_for_shard(shard_id):
            raise KeyError(f"Shard {shard_id} not owned by server {self.server_id}")
        
        # If this server is not primary, forward the request to the primary
        if not self._is_primary_for_shard(shard_id):
            primary_id = self._get_replica_group(shard_id)[0]
            primary_server = self.cfg.kvservers[primary_id]
            # Forward via direct method call to avoid additional network delays
            return primary_server.Put(args)
        
        op_id = (args.client_id, args.operation_id)
        with self.mu:
            if op_id in self.processed_ops:
                return PutAppendReply(self.last_reply[op_id])

            # Apply write
            self.kv_store[args.key] = args.value
            self.processed_ops.add(op_id)
            self.last_reply[op_id] = ""  # Put returns empty string
            reply.value = ""
        
        # Replicate to other servers in the replica group (best-effort, asynchronous)
        replica_group = self._get_replica_group(shard_id)
        for replica_server_id in replica_group[1:]:
            try:
                replica_server = self.cfg.kvservers[replica_server_id]
                replica_args = PutAppendArgs(args.key, args.value)
                replica_args.client_id = args.client_id
                replica_args.operation_id = args.operation_id
                replica_args.last_operation_id = args.last_operation_id
                replica_server.call("KVServer.PutReplica", replica_args)
            except Exception:
                # Ignore replication failures in unreliable network
                pass
        
        return reply

    def PutReplica(self, args: PutAppendArgs):
        """Called only from primary to replicate Put operations."""
        reply = PutAppendReply("")
        
        op_id = (args.client_id, args.operation_id)
        with self.mu:
            if op_id in self.processed_ops:
                return PutAppendReply(self.last_reply[op_id])

            self.kv_store[args.key] = args.value
            self.processed_ops.add(op_id)
            self.last_reply[op_id] = ""
            reply.value = ""
        return reply

    def Append(self, args: PutAppendArgs):
        reply = PutAppendReply("")
        shard_id = self._get_shard_id(args.key)
        
        # Check if this server is responsible for this shard
        if not self._is_replica_for_shard(shard_id):
            raise KeyError(f"Shard {shard_id} not owned by server {self.server_id}")
        
        # If this server is not primary, forward the request to the primary
        if not self._is_primary_for_shard(shard_id):
            primary_id = self._get_replica_group(shard_id)[0]
            primary_server = self.cfg.kvservers[primary_id]
            return primary_server.Append(args)
        
        op_id = (args.client_id, args.operation_id)
        with self.mu:
            if op_id in self.processed_ops:
                return PutAppendReply(self.last_reply[op_id])

            current_value = self.kv_store.get(args.key, "")
            new_value = current_value + args.value
            self.kv_store[args.key] = new_value
            self.processed_ops.add(op_id)
            self.last_reply[op_id] = current_value
            reply.value = current_value
        
        # Replicate to other servers in the replica group (best-effort, asynchronous)
        replica_group = self._get_replica_group(shard_id)
        for replica_server_id in replica_group[1:]:
            try:
                replica_server = self.cfg.kvservers[replica_server_id]
                replica_args = PutAppendArgs(args.key, args.value)
                replica_args.client_id = args.client_id
                replica_args.operation_id = args.operation_id
                replica_args.last_operation_id = args.last_operation_id
                replica_server.call("KVServer.AppendReplica", replica_args)
            except Exception:
                # Ignore replication failures in unreliable network
                pass
        
        return reply

    def AppendReplica(self, args: PutAppendArgs):
        """Called only from primary to replicate Append operations."""
        reply = PutAppendReply("")
        
        op_id = (args.client_id, args.operation_id)
        with self.mu:
            if op_id in self.processed_ops:
                return PutAppendReply(self.last_reply[op_id])

            current_value = self.kv_store.get(args.key, "")
            new_value = current_value + args.value
            self.kv_store[args.key] = new_value
            self.processed_ops.add(op_id)
            self.last_reply[op_id] = current_value
            reply.value = current_value
        return reply
