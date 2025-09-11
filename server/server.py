import rpyc
from rpyc.utils.server import ThreadedServer

TEXT_FILE = "/data/data.txt"

# define a service class for remote tasks
class CountWordsService(rpyc.Service):
    # calculate the number of occurrences of a specific word in a text file
    def exposed_count_words(self, keyword: str) -> int:
        count = 0
        with open(TEXT_FILE, "r", encoding="utf-8") as f:
            for line in f:
                count += line.count(keyword)
        return count


if __name__ == "__main__":
    print("Task server started on port 18861...")
    server = ThreadedServer(CountWordsService, port=18861)
    server.start()