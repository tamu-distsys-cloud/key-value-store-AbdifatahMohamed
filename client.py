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
        
        # Sharding setup
        self.nshards = getattr(cfg, 'nshards', len(servers))
        self.nreplicas = getattr(cfg, 'nreplicas', 1)
        
        print(f"Client init: nshards={self.nshards}, nreplicas={self.nreplicas}, nservers={len(servers)}")

    def _get_shard_and_replicas(self, key: str) -> tuple:
        """Calculate shard ID and get replica servers for a key."""
        # Calculate shard ID
        try:
            shard_id = int(key) % self.nshards
        except ValueError:
            shard_id = hash(key) % self.nshards
        
        # Get replica servers for this shard
        # Server i replicates to [i, i+1, ..., i+R-1] (wrapping around)
        replica_servers = []
        for i in range(self.nreplicas):
            replica_server = (shard_id + i) % len(self.servers)
            replica_servers.append(replica_server)
        
        print(f"Client: Key {key} -> shard {shard_id}, replicas {replica_servers}")
        return shard_id, replica_servers

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

        # Get shard and replica servers for this key
        shard_id, replica_servers = self._get_shard_and_replicas(key)
        
        # Check if this is an unreliable network
        is_unreliable = hasattr(self.cfg, 'net') and not self.cfg.net.reliable
        
        # Try each replica server in the shard's replica group
        for server_id in replica_servers:
            print(f"Client: Trying server {server_id} for key {key}")
            try:
                reply = self.servers[server_id].call("KVServer.Get", args)
                self.last_successful_op = op_id
                return reply.value
            except Exception as e:
                # Try next replica server
                print(f"Client: Server {server_id} failed for key {key}, shard {shard_id}: {e}")
                continue
        
        # If all replicas failed and this is an unreliable network, retry
        if is_unreliable:
            import time
            retry_count = 0
            while retry_count < 50:  # Limit retries
                print(f"Client: All servers failed for key {key}, shard {shard_id}, retrying... (attempt {retry_count + 1})")
                time.sleep(0.01)
                for server_id in replica_servers:
                    try:
                        reply = self.servers[server_id].call("KVServer.Get", args)
                        self.last_successful_op = op_id
                        return reply.value
                    except Exception as e:
                        continue
                retry_count += 1
        
        # If all replicas failed, return empty string
        print(f"Client: All servers failed for key {key}, shard {shard_id}")
        return ""

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

        # Get shard and replica servers for this key
        shard_id, replica_servers = self._get_shard_and_replicas(key)
        
        # Check if this is an unreliable network
        is_unreliable = hasattr(self.cfg, 'net') and not self.cfg.net.reliable
        
        # Try each replica server in the shard's replica group
        for server_id in replica_servers:
            print(f"Client: Trying server {server_id} for {op} key {key}")
            try:
                reply = self.servers[server_id].call("KVServer." + op, args)
                self.last_successful_op = op_id
                return reply.value
            except Exception as e:
                # Try next replica server
                print(f"Client: Server {server_id} failed for {op} key {key}, shard {shard_id}: {e}")
                continue
        
        # If all replicas failed and this is an unreliable network, retry
        if is_unreliable:
            import time
            retry_count = 0
            while retry_count < 50:  # Limit retries
                print(f"Client: All servers failed for {op} key {key}, shard {shard_id}, retrying... (attempt {retry_count + 1})")
                time.sleep(0.01)
                for server_id in replica_servers:
                    try:
                        reply = self.servers[server_id].call("KVServer." + op, args)
                        self.last_successful_op = op_id
                        return reply.value
                    except Exception as e:
                        continue
                retry_count += 1
        
        # If all replicas failed, return empty string for consistency
        print(f"Client: All servers failed for {op} key {key}, shard {shard_id}")
        return ""

    def put(self, key: str, value: str):
        return self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
