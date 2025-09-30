import rpyc
from rpyc.utils.server import ThreadedServer
import redis
import os
import sys

TEXT_FILE = "/data/dune.txt"

# connect to Redis server
r = redis.Redis(host="redis", port=6379, decode_responses=True)

# define a service class for remote tasks
class CountWordsService(rpyc.Service):
    # calculate the number of occurrences of a specific word in a text file
    def exposed_count_words(self, keyword: str) -> int:

        # check if the result is cached in Redis
        cached = r.get(keyword)
        if cached is not None:
            print(f"Cache hit for '{keyword}'", flush=True)
            return int(cached)

        # if not cached, perform the word count
        try:
            with open(TEXT_FILE, "r", encoding="utf-8") as f:
                text = f.read()
            count = text.count(keyword)
        except FileNotFoundError:
            return f"File {TEXT_FILE} not found."

        # store the result in Redis cache
        r.set(keyword, count)
        print(f"Cache miss. Stored '{keyword}' -> {count}", flush=True)
        return count


if __name__ == "__main__":
    args = [a for a in sys.argv if a.startswith("--id=")]
    if not args:
        raise RuntimeError("Illegal server id")
    server_id = int(args[0].split("=")[1])

    port = 18861 + server_id
    print(f"Server {server_id} started on port {port}...", flush=True)
    server = ThreadedServer(CountWordsService, port=port)
    server.start()