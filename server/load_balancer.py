import asyncio
from asyncore import loop
from unittest import result
import rpyc
from rpyc.utils.server import ThreadedServer
import threading

# servers: (host, port)
servers = [
    ("server1", 18862),
    ("server2", 18863),
    ("server3", 18864),
]

# Load Balancing Strategy 1: Least Connections
# track active connections for each server
# active_connections = {i: 0 for i in range(len(servers))}

# def choose_server():
#     return min(active_connections, key=lambda i: active_connections[i])

# Load Balancing Strategy 2: Round Robin
current_index = -1
lock = threading.Lock()

def choose_server():
    global current_index
    with lock:
        current_index = (current_index + 1) % len(servers)
    return current_index

class LoadBalancerService(rpyc.Service):
    async def _dispatch(self, keyword: str) -> int:
        idx = choose_server()
        host, port = servers[idx]

        # increase active connection count
        # active_connections[idx] += 1
        try:
            result = await asyncio.to_thread(
                lambda: rpyc.connect(host, port).root.count_words(keyword)
            )
            # print(f"[LB] '{keyword}' → {host}:{port}, active={active_connections[idx]}", flush=True)
            print(f"[LB] '{keyword}' → {host}:{port}", flush=True)
            return result
        except Exception as e:
            return f"server {host}:{port} failed: {e}"
        # finally:
            # request finished, decrease active connection count
            # active_connections[idx] -= 1

    def exposed_count_words(self, keyword: str) -> int:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(self._dispatch(keyword))
        loop.close()
        return result


if __name__ == "__main__":
    print("Load balancer started on port 18861...", flush=True)
    server = ThreadedServer(LoadBalancerService, port=18861)
    server.start()