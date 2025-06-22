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
        self.kv_store = {}
        self.processed_ops = set()
        self.last_reply = {}

        # Server ID and configuration
        self.server_id = len(cfg.kvservers) - 1  # Assuming sequential launch
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
        """Get the replica group for a shard."""
        total_servers = len(self.cfg.kvservers)
        replica_group = []
        for i in range(self.nreplicas):
            replica_server = (shard_id + i) % total_servers
            replica_group.append(replica_server)
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
        
        # Only primary can handle Get operations for linearizability
        if not self._is_primary_for_shard(shard_id):
            raise Exception("Not primary")
        
        # Check for duplicate operation
        op_id = (args.client_id, args.operation_id)
        if op_id in self.processed_ops:
            # Return cached response for idempotency
            reply.value = self.last_reply.get(op_id, "")
            return reply
        
        self.mu.acquire()
        try:
            reply.value = self.kv_store.get(args.key, "")
            self.processed_ops.add(op_id)
            self.last_reply[op_id] = reply.value
        finally:
            self.mu.release()
        return reply

    def Put(self, args: PutAppendArgs):
        reply = PutAppendReply("")
        shard_id = self._get_shard_id(args.key)
        
        # Check if this server is responsible for this shard
        if not self._is_replica_for_shard(shard_id):
            raise KeyError(f"Shard {shard_id} not owned by server {self.server_id}")
        
        # Only primary can handle client Put operations
        if not self._is_primary_for_shard(shard_id):
            raise Exception("Not primary")
        
        # Check for duplicate operation
        op_id = (args.client_id, args.operation_id)
        if op_id in self.processed_ops:
            # Return cached response for idempotency
            reply.value = self.last_reply.get(op_id, "")
            return reply
        
        self.mu.acquire()
        try:
            # Apply the operation locally first (linearizability)
            self.kv_store[args.key] = args.value
            self.processed_ops.add(op_id)
            reply.value = ""
            self.last_reply[op_id] = ""  # Cache empty response for Put
        finally:
            self.mu.release()
        
        # Replicate to other servers in the replica group (best-effort, asynchronous)
        replica_group = self._get_replica_group(shard_id)
        for replica_server_id in replica_group[1:]:
            try:
                replica_server = self.cfg.kvservers[replica_server_id]
                replica_args = PutAppendArgs(args.key, args.value)
                replica_args.client_id = args.client_id
                replica_args.operation_id = args.operation_id
                replica_args.last_operation_id = args.last_operation_id
                replica_server.PutReplica(replica_args)
            except Exception:
                # Ignore replication failures in unreliable network
                pass
        
        return reply

    def PutReplica(self, args: PutAppendArgs):
        """Called only from primary to replicate Put operations."""
        reply = PutAppendReply("")
        
        # Check for duplicate operation
        op_id = (args.client_id, args.operation_id)
        if op_id in self.processed_ops:
            reply.value = self.last_reply.get(op_id, "")
            return reply
        
        self.mu.acquire()
        try:
            # Apply the operation locally
            self.kv_store[args.key] = args.value
            self.processed_ops.add(op_id)
            reply.value = ""
            self.last_reply[op_id] = ""  # Cache empty response for Put
        finally:
            self.mu.release()
        return reply

    def Append(self, args: PutAppendArgs):
        reply = PutAppendReply("")
        shard_id = self._get_shard_id(args.key)
        
        # Check if this server is responsible for this shard
        if not self._is_replica_for_shard(shard_id):
            raise KeyError(f"Shard {shard_id} not owned by server {self.server_id}")
        
        # Only primary can handle client Append operations
        if not self._is_primary_for_shard(shard_id):
            raise Exception("Not primary")
        
        # Check for duplicate operation
        op_id = (args.client_id, args.operation_id)
        if op_id in self.processed_ops:
            # Return cached response for idempotency
            reply.value = self.last_reply.get(op_id, "")
            return reply
        
        self.mu.acquire()
        try:
            # Apply the operation locally first (linearizability)
            current_value = self.kv_store.get(args.key, "")
            new_value = current_value + args.value
            self.kv_store[args.key] = new_value
            self.processed_ops.add(op_id)
            reply.value = current_value
            self.last_reply[op_id] = current_value  # Cache the response
        finally:
            self.mu.release()
        
        # Replicate to other servers in the replica group (best-effort, asynchronous)
        replica_group = self._get_replica_group(shard_id)
        for replica_server_id in replica_group[1:]:
            try:
                replica_server = self.cfg.kvservers[replica_server_id]
                replica_args = PutAppendArgs(args.key, args.value)
                replica_args.client_id = args.client_id
                replica_args.operation_id = args.operation_id
                replica_args.last_operation_id = args.last_operation_id
                replica_server.AppendReplica(replica_args)
            except Exception:
                # Ignore replication failures in unreliable network
                pass
        
        return reply

    def AppendReplica(self, args: PutAppendArgs):
        """Called only from primary to replicate Append operations."""
        reply = PutAppendReply("")
        
        # Check for duplicate operation
        op_id = (args.client_id, args.operation_id)
        if op_id in self.processed_ops:
            reply.value = self.last_reply.get(op_id, "")
            return reply
        
        self.mu.acquire()
        try:
            # Apply the operation locally
            current_value = self.kv_store.get(args.key, "")
            new_value = current_value + args.value
            self.kv_store[args.key] = new_value
            self.processed_ops.add(op_id)
            reply.value = current_value
            self.last_reply[op_id] = current_value  # Cache the response
        finally:
            self.mu.release()
        return reply
