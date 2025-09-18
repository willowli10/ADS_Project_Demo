import rpyc
import time

def main():
    # connect to the RPyC server
    conn = rpyc.connect("rpyc_server", 18861)
    remote = conn.root

    for word in ("dune", "sand", "banana"):
        start = time.time()
        count = remote.count_words(word)
        print(f"Count of '{word}': {count}, latency: {(time.time() - start) * 1000} ms")

if __name__ == "__main__":
    main()