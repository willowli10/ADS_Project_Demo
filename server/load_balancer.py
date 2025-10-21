import asyncio
import contextlib

# servers: (host, port)
servers = [
    ("server1", 18862),
    ("server2", 18863),
    ("server3", 18864),
]

# Health monitoring config
HEALTH_INTERVAL = 2.0   # seconds between probes
HEALTH_TIMEOUT  = 1.0   # connect timeout per probe
FAIL_THRESHOLD  = 2     # consecutive failures
PASS_THRESHOLD  = 2     # consecutive passes

# Health state per server
server_state = {
    server: {"healthy": True, "fails": 0, "passes": 0, "last_err": ""}
    for server in servers
}

def healthy_servers():
    """Return servers currently considered healthy, with original order."""
    return [server for server in servers if server_state[server]["healthy"]]

# Load Balancing Strategy 1: Round Robin
class RoundRobin:
    def __init__(self):
        self.index = 0

    def getServer(self):
        pool = healthy_servers()
        if not pool:
            return None
        self.index %= len(pool)
        host, port = pool[self.index]
        self.index = (self.index + 1) % len(pool)
        return host, port
    
# Load Balancing Strategy 2: Weighted Round Robin
server_weights = [3, 2, 1]
class WeightedRoundRobin:
    def __init__(self):
        self.index = 0
        self._rebuild()
    
    def _rebuild(self):
        """Rebuild the expanded server list based on current health and weights."""
        self.expanded_servers = []
        for server, weight in zip(servers, server_weights):
            if server_state[server]["healthy"]:
                self.expanded_servers.extend([server] * weight)

    def getServer(self):
        # Rebuild each time in case health changed
        self._rebuild()
        if not self.expanded_servers:
            return None
        self.index %= len(self.expanded_servers)    # Ensure index is in range
        host, port = self.expanded_servers[self.index]  # Select server
        self.index = (self.index + 1) % len(self.expanded_servers)  # Advance index
        return host, port

# current_algo = RoundRobin()
current_algo = WeightedRoundRobin()

async def _pipe_stream(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Continuously forward bytes from reader to writer until EOF or error."""
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                try:
                    writer.write_eof()
                except Exception:
                    # Some transports don't support write_eof
                    pass
                break
            writer.write(data)
            await writer.drain()
    except Exception:
        # Silently close on any transport error
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass
        try:
            await writer.wait_closed()
        except Exception:
            pass

async def _probe_once(host: str, port: int):
    ok = True
    err = ""
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=HEALTH_TIMEOUT)
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
    except Exception as e:
        ok = False
        err = str(e)

    state = server_state[(host, port)]
    if ok:
        state["passes"] += 1
        state["fails"] = 0
        state["last_err"] = ""
        if not state["healthy"] and state["passes"] >= PASS_THRESHOLD:
            state["healthy"] = True
            print(f"[LB] Server {(host, port)} marked UP", flush=True)
    else:
        state["fails"] += 1
        state["passes"] = 0
        state["last_err"] = err
        if state["healthy"] and state["fails"] >= FAIL_THRESHOLD:
            state["healthy"] = False
            print(f"[LB] Server {(host, port)} marked DOWN: {err}", flush=True)

async def _health_loop():
    while True:
        await asyncio.gather(*(_probe_once(h, p) for (h, p) in servers))
        await asyncio.sleep(HEALTH_INTERVAL)

async def _handle_client(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
    """Accept an incoming client socket and proxy it to a chosen backend server."""
    peer = client_writer.get_extra_info("peername")

    # Choose backend according to the balancing strategy (health-aware now)
    choice = current_algo.getServer()
    if choice is None:
        print(f"[LB] No healthy backends; refusing {peer}", flush=True)
        try:
            client_writer.close()
            await client_writer.wait_closed()
        except Exception:
            pass
        return

    host, port = choice

    # Try to connect; on failure mark DOWN and try next healthy server immediately
    attempts = 0
    max_attempts = len(servers)
    while attempts < max_attempts:
        try:
            server_reader, server_writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=HEALTH_TIMEOUT
            )
            break
        except Exception as e:
            # mark server as failed
            st = server_state[(host, port)]
            st["fails"] = max(st["fails"], FAIL_THRESHOLD)  # push over threshold
            if st["healthy"]:
                st["healthy"] = False
                print(f"[LB] Marking DOWN on connect error {host}:{port} ({e})", flush=True)

            # pick next healthy
            choice = current_algo.getServer()
            if choice is None:
                print(f"[LB] No healthy backends after retries; closing {peer}", flush=True)
                try:
                    client_writer.close()
                    await client_writer.wait_closed()
                except Exception:
                    pass
                return
            host, port = choice
            attempts += 1

    print(f"[LB] New connection {peer} → {host}:{port}", flush=True)

    # Bi-directional piping between client and chosen backend
    to_server = asyncio.create_task(_pipe_stream(client_reader, server_writer))
    to_client = asyncio.create_task(_pipe_stream(server_reader, client_writer))

    # When either direction finishes, cancel the other and cleanup
    done, pending = await asyncio.wait({to_server, to_client}, return_when=asyncio.FIRST_COMPLETED)

    for task in pending:
        task.cancel()
        try:
            await task
        except Exception:
            pass

    print(f"[LB] Closed {peer} ↔ {host}:{port}", flush=True)


async def main():
    server = await asyncio.start_server(_handle_client, host="0.0.0.0", port=18861)
    socks = server.sockets or []
    addrs = ", ".join(str(s.getsockname()) for s in socks)
    print(f"Load balancer started on {addrs}...", flush=True)

    # start health monitor
    health_task = asyncio.create_task(_health_loop())
    try:
        async with server:
            await server.serve_forever()
    finally:
        health_task.cancel()
        with contextlib.suppress(Exception):
            await health_task


if __name__ == "__main__":
    asyncio.run(main())
