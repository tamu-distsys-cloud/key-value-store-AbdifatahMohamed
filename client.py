import threading
import time
from typing import Any, List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg

        # Your definitions here.
        self.current_server = 0

        # Generate a globally-unique Clerk ID (object ids can be reused)
        global _client_id_counter, _client_id_lock
        try:
            _client_id_lock
        except NameError:
            import threading
            _client_id_lock = threading.Lock()
            _client_id_counter = 0

        with _client_id_lock:
            self.client_id = _client_id_counter
            _client_id_counter += 1
        
        self.seq_num = 0
        self.last_successful_op = 0
        
        # Sharding setup - use the config's nshards if available, otherwise infer from servers
        if hasattr(cfg, 'nshards'):
            self.nshards = cfg.nshards
        else:
            # If no nshards specified, assume single shard per server
            self.nshards = len(servers)
        
        self.nreplicas = getattr(cfg, 'nreplicas', 1)
        self.total_servers = len(servers)
        
        print(f"Client init: nshards={self.nshards}, nreplicas={self.nreplicas}, nservers={self.total_servers}")

    def _get_shard_id(self, key: str) -> int:
        """Calculate shard ID for a key using int(key) % nshards (same as server logic)."""
        try:
            shard_id = int(key) % self.nshards
        except ValueError:
            shard_id = hash(key) % self.nshards
        return shard_id

    def _get_replica_group(self, shard_id: int) -> List[int]:
        """Get the replica group for a shard using [(shard_id + i) % total_servers for i in range(nreplicas)] (same as server logic)."""
        replica_group = [(shard_id + i) % self.total_servers for i in range(self.nreplicas)]
        return replica_group

    def next_op_id(self) -> int:
        """Get the next operation ID and increment."""
        op_id = self.seq_num
        self.seq_num += 1
        return op_id

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
        op_id = self.next_op_id()
        args = GetArgs(key)
        args.client_id = self.client_id
        args.operation_id = op_id
        args.last_operation_id = self.last_successful_op
        
        # Calculate shard ID and replica group using same logic as server
        shard_id = self._get_shard_id(key)
        replica_group = self._get_replica_group(shard_id)
        
        # Retry logic: try each server in replica group, then retry entire process
        max_retries = 10000  # Set a maximum retry limit
        retries = 0
        while retries < max_retries:
            for server_id in replica_group:
                try:
                    reply = self.servers[server_id].call("KVServer.Get", args)
                    if reply is not None:
                        self.last_successful_op = op_id
                        return reply.value
                    # If reply is None, continue to next server
                    continue
                except KeyError as e:
                    # Shard not owned by this server, try next server
                    if "not owned" in str(e):
                        continue
                    # Re-raise other KeyErrors
                    raise
                except Exception as e:
                    # If it's "Not primary" error or any other error, try next server
                    if "Not primary" in str(e):
                        continue
                    # For other exceptions, also try next server
                    continue
            
            # If all servers in replica group failed, sleep and retry
            time.sleep(0.05)
            retries += 1
        raise TimeoutError("Max retries reached")

    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.servers[i].call("KVServer."+op, args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def put_append(self, key: str, value: str, op: str):
        op_id = self.next_op_id()
        args = PutAppendArgs(key, value)
        args.client_id = self.client_id
        args.operation_id = op_id
        args.last_operation_id = self.last_successful_op
        
        # Calculate shard ID and replica group using same logic as server
        shard_id = self._get_shard_id(key)
        replica_group = self._get_replica_group(shard_id)
        
        # Retry logic: try each server in replica group, then retry entire process
        while True:
            for server_id in replica_group:
                try:
                    reply = self.servers[server_id].call("KVServer." + op, args)
                    if reply is not None:
                        self.last_successful_op = op_id
                        return reply.value
                    # If reply is None, continue to next server
                    continue
                except KeyError as e:
                    # Shard not owned by this server, try next server
                    if "not owned" in str(e):
                        continue
                    # Re-raise other KeyErrors
                    raise
                except Exception as e:
                    # If it's "Not primary" error or any other error, try next server
                    if "Not primary" in str(e):
                        continue
                    # For other exceptions, also try next server
                    continue
            
            # If all servers in replica group failed, sleep and retry
            time.sleep(0.05)

    def put(self, key: str, value: str):
        return self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
