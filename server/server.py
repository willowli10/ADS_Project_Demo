import rpyc
from rpyc.utils.server import ThreadedServer
import redis

TEXT_FILE = "/data/data.txt"

# connect to Redis server
r = redis.Redis(host="redis", port=6379, decode_responses=True)

# define a service class for remote tasks
class CountWordsService(rpyc.Service):
    # calculate the number of occurrences of a specific word in a text file
    def exposed_count_words(self, keyword: str) -> int:

        # check if the result is cached in Redis
        cached = r.get(keyword)
        if cached is not None:
            print(f"Cache hit for '{keyword}'")
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
        print(f"Cache miss. Stored '{keyword}' -> {count}")
        return count


if __name__ == "__main__":
    print("Task server started on port 18861...")
    server = ThreadedServer(CountWordsService, port=18861)
    server.start()