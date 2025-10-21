import asyncio
import threading

# servers: (host, port)
servers = [
    ("server1", 18862),
    ("server2", 18863),
    ("server3", 18864),
]

# Load Balancing Strategy 1: Round Robin
class RoundRobin:
    def __init__(self):
        self.index = 0

    def getServer(self):
        host, port = servers[self.index]
        self.index = (self.index + 1) % len(servers)
        return host, port
    
# Load Balancing Strategy 2: Weighted Round Robin
server_weights = [3, 2, 1]
class WeightedRoundRobin:
    def __init__(self):
        self.index = 0
        self.expanded_servers = []
        for server, weight in zip(servers, server_weights):
            self.expanded_servers.extend([server] * weight)

    def getServer(self):
        host, port = self.expanded_servers[self.index]
        self.index = (self.index + 1) % len(self.expanded_servers)
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


async def _handle_client(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
    """Accept an incoming client socket and proxy it to a chosen backend server."""
    peer = client_writer.get_extra_info("peername")

    # Choose backend according to the balancing strategy
    host, port = current_algo.getServer()

    try:
        server_reader, server_writer = await asyncio.open_connection(host, port)
    except Exception as e:
        print(f"[LB] Failed to connect {peer} -> {host}:{port}: {e}", flush=True)
        try:
            client_writer.close()
            await client_writer.wait_closed()
        except Exception:
            pass
        return

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
    # The load balancer now acts as a transparent TCP proxy. Clients still speak RPyC,
    # and the LB forwards raw bytes to one of the backend RPyC servers.
    server = await asyncio.start_server(_handle_client, host="0.0.0.0", port=18861)
    socks = server.sockets or []
    addrs = ", ".join(str(s.getsockname()) for s in socks)
    print(f"Load balancer started on {addrs}...", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
